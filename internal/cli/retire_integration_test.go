// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// retire writes page_counts@v1's tombstone on DynamoDB Local. Then a projector whose catalog
// lists the version retired advances a unit that holds the version's fence past it.
func TestRetireOnDynamoDBLocal(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := &retireStore{dir: writeProject(t, "retire")}
	s.generate(t, 1)
	data := s.generate(t, 2)
	res := compile(t, s.dir)
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	must(t, err)
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	naming := emulatortest.Tables(t, cfg, m)
	b := dynamo.New(dynamodb.NewFromConfig(cfg))
	mt := meta.New(b, naming.MetaTable())
	_, err = mt.RegisterCatalog(ctx, data, "v0.5.0")
	must(t, err)
	makeLive(t, mt, retiring)
	makeLive(t, mt, "page_counts@v2")
	const unit = "shard-1"
	u, err := mt.Unit(ctx, "page_views", unit)
	must(t, err)
	_, err = mt.EnsureFence(ctx, u, retiring)
	must(t, err)

	var stdout, stderr strings.Builder
	args := []string{"retire", retiring, "-endpoint", *cfg.BaseEndpoint, "-prefix", naming.Prefix, "-yes"}
	if status := cli.Run(ctx, args, nil, &stdout, &stderr, s.dir); status != 0 {
		t.Fatalf("retire: status %d\n%s%s", status, stdout.String(), stderr.String())
	}
	states, err := mt.ViewStates(ctx)
	must(t, err)
	if vs := states[retiring]; vs.State != meta.Retired || vs.RetiredAt.IsZero() {
		t.Errorf("%s: %+v, want retired", retiring, vs)
	}

	after, err := catalog.Marshal(compile(t, s.dir).Catalog)
	must(t, err)
	cat, err := engine.Compile(after)
	must(t, err)
	if got := cat.Catalog().Retired; len(got) != 1 || got[0] != retiring {
		t.Fatalf("the catalog after retire lists %q retired, want [%s]", got, retiring)
	}
	e := engine.New(b, engine.Options{Region: emulatortest.Region, Prefix: naming.Prefix})
	out, err := e.Append(ctx, cat, "page_views", [][]expr.Value{{expr.String("u1"), expr.String("/home"), expr.Null()}}, engine.AppendMode{})
	if err == nil {
		err = out[0].Err
	}
	must(t, err)
	it, err := b.Get(ctx, naming.StreamTable("page_views"), out[0].Key, true)
	must(t, err)
	const seq = "100000000000000000005"
	core := projector.New(cat.Projector(), b, nil, projector.Config{Naming: naming})
	hctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	r, err := core.Handle(hctx, projector.Batch{Stream: "page_views", Unit: unit, Records: []projector.Record{{Op: projector.Insert, Seq: seq, New: it}}})
	if err != nil || r.FailedSeq != "" {
		t.Fatalf("Handle = %+v, %v; want success", r, err)
	}
	u, err = mt.Unit(ctx, "page_views", unit)
	must(t, err)
	padded, err := keys.PadSeq(seq)
	must(t, err)
	if u.Progress != padded || u.Fences[retiring] != meta.ZeroFence() {
		t.Errorf("unit progress %s and %s's fence %s, want %s past the fence %s", u.Progress, retiring, u.Fences[retiring], padded, meta.ZeroFence())
	}
}
