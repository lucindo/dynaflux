// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package localstore_test

import (
	"context"
	"crypto/rand"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/localstore"
	"github.com/lucindo/dynaflux/internal/validate"
)

// Ensure creates only the tables that are missing, and Delete deletes only those that
// exist: dynaflux dev runs again on a store, and deletes one that is partly gone.
func TestEnsureAndDelete(t *testing.T) {
	t.Parallel()
	cfg, cat, ds := validate.Load(fstest.MapFS{
		"dynaflux.yaml": {Data: []byte("store: analytics\nregions: [us-east-1]\n")},
		"dynaflux/streams.sql": {Data: []byte(`CREATE STREAM page_views (k String) PRIMARY KEY (k);
CREATE MATERIALIZED VIEW daily ORDER BY (k) PRIMARY KEY (k) AS SELECT k, count() AS n FROM page_views GROUP BY k;`)},
	}, "/project")
	if cat == nil {
		t.Fatalf("the fixture doesn't compile: %v", ds)
	}
	m, err := infra.Build(cfg, cat, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	p := "l" + strings.ToLower(rand.Text()[:12]) + "-"
	ts, err := localstore.Tables(m, p)
	if err != nil {
		t.Fatal(err)
	}
	all := []string{p + "analytics-stream-page_views", p + "analytics-meta", p + "analytics-view-daily-v1"}
	var names []string
	for _, tb := range ts {
		names = append(names, tb.Name)
	}
	if !slices.Equal(names, all) {
		t.Fatalf("Tables: %q, want %q", names, all)
	}
	c := dynamodb.NewFromConfig(emulatortest.Config(t, nil))
	ctx := context.WithoutCancel(t.Context()) // t.Context is canceled before cleanups run
	t.Cleanup(func() {
		if _, derr := localstore.Delete(ctx, c, ts); derr != nil {
			t.Error(derr)
		}
	})

	steps := []struct {
		name string
		do   func() ([]string, error)
		want []string
	}{
		{"ensure one", func() ([]string, error) { return localstore.Ensure(ctx, c, ts[1:2]) }, all[1:2]},
		{"ensure all", func() ([]string, error) { return localstore.Ensure(ctx, c, ts) }, []string{all[0], all[2]}},
		{"ensure again", func() ([]string, error) { return localstore.Ensure(ctx, c, ts) }, nil},
		{"delete one", func() ([]string, error) { return localstore.Delete(ctx, c, ts[:1]) }, all[:1]},
		{"delete all", func() ([]string, error) { return localstore.Delete(ctx, c, ts) }, all[1:]},
		{"delete again", func() ([]string, error) { return localstore.Delete(ctx, c, ts) }, nil},
	}
	for _, st := range steps {
		if got, serr := st.do(); serr != nil || !slices.Equal(got, st.want) {
			t.Fatalf("%s: %q, %v; want %q", st.name, got, serr, st.want)
		}
	}
}
