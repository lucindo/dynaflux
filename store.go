// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux

import (
	"errors"
	"time"

	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/engine"
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
	})
}

// Store is a Dynaflux store that the generated client appends to and reads. In tests,
// fluxtest.NewStore makes one in memory. The zero Store can't be used.
type Store struct {
	e *engine.Engine
}

// Catalog holds a store's definitions, compiled. The generated package declares one, as
// Catalog in its catalog.go. The zero Catalog can't be used.
type Catalog struct {
	c   *engine.Catalog
	err error // why the generated catalog can't be loaded
}

// Token names the events that an append stored. The zero Token names none.
type Token struct {
	s string
}

// String returns the token's text form, which is opaque.
func (t Token) String() string { return t.s }

// IsZero reports whether t is the zero Token.
func (t Token) IsZero() bool { return t.s == "" }

// AppendOption configures an append. There are none yet; every generated append method
// takes them, so adding one won't change the methods' types.
type AppendOption func(*appendOptions)

type appendOptions struct{}

// QueryOption configures a query. There are none yet; every generated query method takes
// them, so adding one won't change the methods' types.
type QueryOption func(*queryOptions)

type queryOptions struct{}

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
