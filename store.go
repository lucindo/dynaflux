// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

//nolint:gochecknoinits // registers the functions fluxgen and fluxtest reach this package's types through
func init() {
	//nolint:errcheck // fluxgen and fluxtest pass this package's types; any other is a bug there
	access.Register(access.Funcs{
		InitStore: func(dst any, e *engine.Engine) { dst.(*Store).e = e },
		StoreEngine: func(s any) *engine.Engine {
			if s := s.(*Store); s != nil {
				return s.e
			}
			return nil
		},
		InitCatalog: func(dst any, c *engine.Catalog, err error) { *dst.(*Catalog) = Catalog{c: c, err: err} },
		Catalog: func(c any) (*engine.Catalog, error) {
			if cat := c.(*Catalog); cat != nil && (cat.c != nil || cat.err != nil) {
				return cat.c, cat.err
			}
			return nil, errors.New("dynaflux: the Catalog is nil or zero; use the generated package's Catalog")
		},
		InitToken: func(dst any, text string) { dst.(*Token).s = text },
		ReadRange: func(opts any) (*time.Time, *time.Time) {
			var o readOptions
			for _, opt := range opts.([]ReadOption) {
				opt(&o)
			}
			return o.since, o.until
		},
		QueryOptions: func(opts any) engine.QueryOptions {
			var o queryOptions
			for _, opt := range opts.([]QueryOption) {
				opt(&o)
			}
			return engine.QueryOptions(o)
		},
		AppendMode: func(opts any) engine.AppendMode {
			var o appendOptions
			for _, opt := range opts.([]AppendOption) {
				opt(&o)
			}
			return engine.AppendMode(o)
		},
	})
}

// Store is a Dynaflux store that the generated client appends to and reads. [Open] returns
// one backed by DynamoDB; in tests, fluxtest.NewStore makes one in memory. The zero Store
// can't be used.
type Store struct {
	e *engine.Engine
}

// Config configures the Store that [Open] returns.
type Config struct {
	// AWS configures the DynamoDB client. Its Region is the region the store appends in,
	// which events record as origin_region. Dynaflux uses its credentials, retryer, HTTP
	// client and endpoint as they are. The projector's client is a model to follow: the
	// standard retryer without its client-side retry quota (ratelimit.None), and a 10 s
	// HTTP timeout for each attempt.
	AWS aws.Config
	// Prefix is the deployment's prefix of resource names: the templates' Prefix
	// parameter, "" for none.
	Prefix string
}

// Open returns a store backed by the DynamoDB tables that the generated templates create
// for cfg.Prefix in cfg.AWS.Region, which must be a region code such as us-east-1: for FIPS
// endpoints, set AWS_USE_FIPS_ENDPOINT instead of naming a FIPS pseudo-region. It makes no
// AWS call, so a missing table or grant shows on the first append, read or query.
//
// A query over a view checks that the view's version is live in the region, in the view
// states it read from the store's meta table at most 30 seconds before, and returns
// [ErrViewNotReady] when it isn't. The application needs dynamodb:Query on the meta table
// for that read.
func Open(cfg Config) (*Store, error) {
	if cfg.AWS.Region == "" {
		return nil, errors.New("dynaflux: Config.AWS has no Region")
	}
	if problem, hint := catalog.CheckRegion(cfg.AWS.Region); problem != "" {
		return nil, fmt.Errorf("dynaflux: Config.AWS.Region %s; %s", problem, hint)
	}
	if !catalog.ValidPrefix(cfg.Prefix) {
		return nil, fmt.Errorf("dynaflux: Config.Prefix is %q; use lowercase letters, digits and -, "+
			"starting with a letter and ending with -, such as prod-, or \"\" for none", cfg.Prefix)
	}
	b := dynamo.New(dynamodb.NewFromConfig(cfg.AWS))
	return &Store{e: engine.New(b, engine.Options{Region: cfg.AWS.Region, Prefix: cfg.Prefix})}, nil
}

// Catalog holds a store's definitions, compiled. The generated package declares one, as
// Catalog in its catalog.go. The zero Catalog can't be used.
type Catalog struct {
	c   *engine.Catalog
	err error // why the generated catalog can't be loaded
}

// Token names the events that an append stored, in the store's region. The zero Token
// names none.
type Token struct {
	s string
}

// String returns the token's text form, which is opaque. [ParseToken] reads it back.
func (t Token) String() string { return t.s }

// IsZero reports whether t is the zero Token.
func (t Token) IsZero() bool { return t.s == "" }

// ParseToken returns the Token whose text form is s, so that another process in the same
// region can wait for its events with [WaitFor]. It checks only the form: a token is
// unauthenticated text that grants nothing, since WaitFor reads only receipts with it.
func ParseToken(s string) (Token, error) {
	if err := engine.CheckToken(s); err != nil {
		return Token{}, fmt.Errorf("dynaflux: %w", err)
	}
	return Token{s: s}, nil
}

// AppendOption configures an append. Every generated append method takes them.
type AppendOption func(*appendOptions)

type appendOptions engine.AppendMode

// Waitable asks the projector of the store's region to write a receipt for each event of
// the append once it has handled it, so that [WaitFor] can wait for them: one more write to
// the store's meta table per event, which other appends don't pay.
func Waitable() AppendOption {
	return func(o *appendOptions) { o.Receipt = true }
}

// QueryOption configures a query.
type QueryOption func(*queryOptions)

type queryOptions engine.QueryOptions

// AllowPartial runs a query over a view whose version isn't live in the store's region,
// without reading its state, instead of returning [ErrViewNotReady]. The rows may miss
// events: those a backfill hasn't applied yet, or every event after the version broke.
func AllowPartial() QueryOption {
	return func(o *queryOptions) { o.AllowPartial = true }
}

// WaitFor makes a query first wait until the projector of the store's region has handled
// every event of tok, an append's token in this region, for at most timeout. A query over a
// view then reads its version's state again, and both kinds read strongly consistently, so
// the rows include the events. A wait takes about the projector's batching window plus one
// invocation, 1 to 1.5 seconds by default: give it a timeout of a few seconds. When the
// timeout passes first, the query returns an error that matches [ErrWaitTimeout]; when ctx
// ends first, ctx's error.
//
// The query fails at once, before any read, in another region than the append's, for an
// event appended without [Waitable], for one that another region stored, as a duplicate of
// a replicated DEDUP BY event is, for a token of more than 1,000 events, and for a token of
// a stream that the store doesn't have. A token can be waited on for 24 hours after its
// append. The zero Token waits for nothing. Given more than once, WaitFor makes the query
// wait for each token in turn, each for its own timeout.
func WaitFor(tok Token, timeout time.Duration) QueryOption {
	return func(o *queryOptions) { o.Wait = append(o.Wait, engine.Wait{Token: tok.s, Timeout: timeout}) }
}

// ReadOption configures a stream read.
type ReadOption func(*readOptions)

type readOptions struct {
	since, until *time.Time
}

// Since reads the events whose event_time is t or later.
func Since(t time.Time) ReadOption {
	return func(o *readOptions) { o.since = &t }
}

// Until reads the events whose event_time is before t. With [Since], windows that share
// an end chain without a gap or a repeat.
func Until(t time.Time) ReadOption {
	return func(o *readOptions) { o.until = &t }
}
