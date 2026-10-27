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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
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
// It fails the test when cfg.Region isn't Region. The tables are deleted when the test ends.
func Tables(tb testing.TB, cfg aws.Config, m *infra.Manifest) catalog.Naming {
	tb.Helper()
	if cfg.Region != Region {
		tb.Fatalf("emulatortest: region %q: DynamoDB Local keeps the suite's tables in %s", cfg.Region, Region)
	}
	prefix := uniquePrefix()
	c := admin(cfg)
	for _, spec := range slices.Concat(m.Global.Tables, m.Regional.Tables) {
		create(tb, c, resolve(tb, spec.Name, prefix), spec)
	}
	return catalog.Naming{Prefix: prefix, Store: m.Store}
}

// Table creates an empty table with Dynaflux's keys, deleted when the test ends, and returns
// its name.
func Table(tb testing.TB, cfg aws.Config) string {
	tb.Helper()
	name := uniquePrefix() + "table"
	create(tb, admin(cfg), name, infra.Table{Keys: []infra.Key{
		{Name: catalog.AttrPK, Type: catalog.KeyAttrType, Key: infra.KeyHash},
		{Name: catalog.AttrSK, Type: catalog.KeyAttrType, Key: infra.KeyRange},
	}})
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

// resolve returns a table name of the manifest with prefix in place of ${prefix}: table
// names hold no other substitution.
func resolve(tb testing.TB, name infra.Text, prefix string) string {
	tb.Helper()
	segs, err := name.Segments()
	if err != nil {
		tb.Fatalf("emulatortest: table name: %v", err)
	}
	var b strings.Builder
	for _, seg := range segs {
		switch {
		case seg.Placeholder == infra.PlaceholderPrefix:
			b.WriteString(prefix)
		case seg.Placeholder == "" && seg.Ref == infra.Ref{}:
			b.WriteString(seg.Literal)
		default:
			tb.Fatalf("emulatortest: table name %q holds a substitution other than ${%s}", name, infra.PlaceholderPrefix)
		}
	}
	return b.String()
}

// create creates table name as spec describes it.
func create(tb testing.TB, c *dynamodb.Client, name string, spec infra.Table) {
	tb.Helper()
	in := &dynamodb.CreateTableInput{TableName: &name, BillingMode: types.BillingModePayPerRequest}
	for _, k := range spec.Keys {
		in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{
			AttributeName: new(k.Name), AttributeType: types.ScalarAttributeType(k.Type),
		})
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: new(k.Name), KeyType: keyType(tb, k.Key)})
	}
	switch spec.StreamView {
	case "":
	case infra.StreamViewNewAndOld:
		in.StreamSpecification = &types.StreamSpecification{
			StreamEnabled: new(true), StreamViewType: types.StreamViewTypeNewAndOldImages,
		}
	default:
		tb.Fatalf("emulatortest: table %s: stream view %q", name, spec.StreamView)
	}
	if _, err := c.CreateTable(tb.Context(), in); err != nil {
		tb.Fatalf("emulatortest: create table %s: %v", name, err)
	}
	// tb.Context is canceled before cleanups run.
	ctx := context.WithoutCancel(tb.Context())
	tb.Cleanup(func() {
		if _, err := c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &name}); err != nil {
			tb.Errorf("emulatortest: delete table %s: %v", name, err)
		}
	})
	if spec.TTLAttribute != "" {
		_, err := c.UpdateTimeToLive(tb.Context(), &dynamodb.UpdateTimeToLiveInput{
			TableName:               &name,
			TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: &spec.TTLAttribute, Enabled: new(true)},
		})
		if err != nil {
			tb.Fatalf("emulatortest: TTL of table %s: %v", name, err)
		}
	}
}

func keyType(tb testing.TB, r infra.KeyRole) types.KeyType {
	tb.Helper()
	switch r {
	case infra.KeyHash:
		return types.KeyTypeHash
	case infra.KeyRange:
		return types.KeyTypeRange
	}
	tb.Fatalf("emulatortest: key role %q", r)
	return ""
}
