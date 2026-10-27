// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package emulatortest connects tests to DynamoDB Local (docs/decisions/0009-emulator-choice.md):
// clients that can reach only a loopback endpoint, tables of their own for each test, and a
// recorder of every request a client sends. Production code never imports it.
package emulatortest

import (
	"context"
	"crypto/rand"
	"iter"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/localstore"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// Region and AccessKeyID are every test client's. DynamoDB Local without -sharedDb keeps one
// database per client region and access key, so a client with another region or key sees
// none of the suite's tables.
const (
	Region      = "us-east-1"
	AccessKeyID = "dynafluxtest" // DynamoDB Local accepts letters and digits only
)

const (
	endpointVar    = "DYNAFLUX_DYNAMODB_ENDPOINT"
	attemptTimeout = 2 * time.Second
)

// Config returns an aws.Config for DYNAFLUX_DYNAMODB_ENDPOINT: loopback only, Region, static
// fake credentials, no retry quota, a 2 s attempt timeout. It fails the test when the
// variable is unset or names another host. The HTTP client is rec when it isn't nil, and
// NewRecorder(nil) keeps the attempt timeout.
func Config(tb testing.TB, rec *Recorder) aws.Config {
	tb.Helper()
	ep := os.Getenv(endpointVar)
	if ep == "" {
		tb.Fatalf("%s is unset: run make integration, or make emulators-up with %s=http://127.0.0.1:8000", endpointVar, endpointVar)
	}
	u, err := url.Parse(ep)
	if err != nil {
		tb.Fatalf("%s: %v", endpointVar, err)
	}
	if ip := net.ParseIP(u.Hostname()); u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		tb.Fatalf("%s is %q, which isn't on loopback: tests never reach AWS", endpointVar, ep)
	}
	cfg := aws.Config{
		Region:       Region,
		BaseEndpoint: &ep,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: AccessKeyID, SecretAccessKey: "fake", Source: "emulatortest"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.RateLimiter = ratelimit.None })
		},
		HTTPClient: timedClient(),
	}
	if rec != nil {
		cfg.HTTPClient = rec
	}
	return cfg
}

// Tables creates every table of the regional and global scopes of m (from internal/infra)
// with a unique prefix: keys, PAY_PER_REQUEST, the stream view and the TTL attribute exactly
// as the manifest says, so a disagreement between the templates and the runtime fails here.
// DynamoDB Local keeps a database per region, so only clients in cfg.Region see the tables.
// The tables are deleted when the test ends.
func Tables(tb testing.TB, cfg aws.Config, m *infra.Manifest) catalog.Naming {
	tb.Helper()
	prefix := uniquePrefix()
	ts, err := localstore.Tables(m, prefix)
	if err != nil {
		tb.Fatalf("emulatortest: %v", err)
	}
	create(tb, cfg, ts)
	return catalog.Naming{Prefix: prefix, Store: m.Store}
}

// Table creates an empty table with Dynaflux's keys, deleted when the test ends, and returns
// its name.
func Table(tb testing.TB, cfg aws.Config) string {
	tb.Helper()
	name := uniquePrefix() + "table"
	create(tb, cfg, []localstore.Table{{Name: name, Spec: infra.Table{Keys: []infra.Key{
		{Name: catalog.AttrPK, Type: catalog.KeyAttrType, Key: infra.KeyHash},
		{Name: catalog.AttrSK, Type: catalog.KeyAttrType, Key: infra.KeyRange},
	}}}})
	return name
}

// Backend returns a dynamo backend over a new [Table] that receives every request, whatever
// table the request names, so a suite written for one table name runs each check on a table
// of its own.
func Backend(tb testing.TB, cfg aws.Config) store.Backend {
	tb.Helper()
	return renamed{dynamo.New(dynamodb.NewFromConfig(cfg)), Table(tb, cfg)}
}

type renamed struct {
	b     *dynamo.Backend
	table string
}

func (r renamed) Get(ctx context.Context, _ string, k store.Key, consistent bool) (store.Item, error) {
	return r.b.Get(ctx, r.table, k, consistent)
}

func (r renamed) BatchGet(ctx context.Context, _ string, ks []store.Key, consistent bool) ([]store.Item, error) {
	return r.b.BatchGet(ctx, r.table, ks, consistent)
}

func (r renamed) Query(ctx context.Context, _ string, q store.Query) iter.Seq2[store.Item, error] {
	return r.b.Query(ctx, r.table, q)
}

func (r renamed) ScanPage(ctx context.Context, _ string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	return r.b.ScanPage(ctx, r.table, s, start)
}

func (r renamed) Put(ctx context.Context, _ string, it store.Item, c store.Cond) error {
	return r.b.Put(ctx, r.table, it, c)
}

// uniquePrefix is a valid deployment prefix that no other test, process or earlier run
// uses, since every run shares one DynamoDB Local.
func uniquePrefix() string {
	return "t" + strings.ToLower(rand.Text()[:12]) + "-"
}

// admin is a client for creating and deleting tables. It bypasses cfg's HTTP client, so a
// recorder there sees only the calls under test.
func admin(cfg aws.Config) *dynamodb.Client {
	cfg.HTTPClient = timedClient()
	return dynamodb.NewFromConfig(cfg)
}

func timedClient() aws.HTTPClient { return awshttp.NewBuildableClient().WithTimeout(attemptTimeout) }

// create creates ts, which are deleted when the test ends.
func create(tb testing.TB, cfg aws.Config, ts []localstore.Table) {
	tb.Helper()
	c := admin(cfg)
	// tb.Context is canceled before cleanups run.
	ctx := context.WithoutCancel(tb.Context())
	tb.Cleanup(func() {
		if _, err := localstore.Delete(ctx, c, ts); err != nil {
			tb.Errorf("emulatortest: %v", err)
		}
	})
	if _, err := localstore.Ensure(tb.Context(), c, ts); err != nil {
		tb.Fatalf("emulatortest: %v", err)
	}
}
