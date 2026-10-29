// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package fluxruntime_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/setup"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// Run in setup mode answers the catalog's Create on aws-lambda-go's loop: it registers the
// catalog it runs in the meta table on DynamoDB Local, and replies with the hash's physical
// ID.
func TestRunRegistersCatalog(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	table := emulatortest.Table(t, cfg)
	data := childCatalog(t)
	hash := catalog.Hash(data)
	props := `{
    "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup",
    "ServiceTimeout": "300",
    "MetaTable": "` + table + `",
    "CatalogHash": "` + hash + `",
    "Generator": "v0.5.0"
  }`
	r := newReplies(t)
	ev, err := json.Marshal(cfnEvent(t, "Create", setup.TypeCatalog, "Catalog", "", props, r.url()))
	if err != nil {
		t.Fatal(err)
	}
	api := newRuntimeAPI(t, newFakeDynamoDB(t), ev)
	api.mu.Lock()
	api.timeout = 900 * time.Second // the setup function's
	api.mu.Unlock()
	c := startChild(t, childRun, api, map[string]string{"DYNAFLUX_MODE": "setup", "AWS_ENDPOINT_URL_DYNAMODB": aws.ToString(cfg.BaseEndpoint)})
	c.serve(t, api)
	want := `{"Status":"SUCCESS","RequestId":"` + requestID + `","LogicalResourceId":"Catalog","StackId":"` + stackID +
		`","PhysicalResourceId":"` + table + "/" + hash + `"}`
	if got := r.all(); len(got) != 1 || got[0].body != want {
		t.Fatalf("replies %+v\nwant\n%s\nstderr:\n%s", got, want, &c.stderr)
	}
	m := meta.New(dynamo.New(dynamodb.NewFromConfig(cfg)), table)
	reg, ok, err := m.Current(t.Context())
	if err != nil || !ok || reg.Hash != hash || reg.Generator != "v0.5.0" || reg.Format != catalog.FormatVersion {
		t.Errorf("current catalog %+v, %t, %v; want %s by v0.5.0 in format %d", reg, ok, err, hash, catalog.FormatVersion)
	}
	stored, err := m.ReadCatalog(t.Context(), hash)
	if err != nil || !bytes.Equal(stored, data) {
		t.Errorf("stored catalog %s, %v; want the child's", stored, err)
	}
}

// Run in setup mode answers ArchiveMode's Create and Update: the update that stops archiving
// a stream records its mode and a hole in the meta table on DynamoDB Local.
func TestRunAppliesArchiveModes(t *testing.T) {
	t.Parallel()
	cfg := emulatortest.Config(t, nil)
	table := emulatortest.Table(t, cfg)
	props := func(mode string) string {
		return `{"ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:prod-analytics-setup", "ServiceTimeout": "300",
    "MetaTable": "` + table + `", "Streams": {"clicks": "` + mode + `"}}`
	}
	r := newReplies(t)
	update := cfnEvent(t, "Update", setup.TypeArchiveMode, "ArchiveMode", table+"/archive-mode", props("off"), r.url())
	update.OldResourceProperties["Streams"] = map[string]any{"clicks": "all"}
	var events [][]byte
	for _, ev := range []any{cfnEvent(t, "Create", setup.TypeArchiveMode, "ArchiveMode", "", props("all"), r.url()), update} {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, data)
	}
	api := newRuntimeAPI(t, newFakeDynamoDB(t), events...)
	api.mu.Lock()
	api.timeout = 900 * time.Second // the setup function's
	api.mu.Unlock()
	c := startChild(t, childRun, api, map[string]string{"DYNAFLUX_MODE": "setup", "AWS_ENDPOINT_URL_DYNAMODB": aws.ToString(cfg.BaseEndpoint)})
	c.serve(t, api)
	reply := `{"Status":"SUCCESS","RequestId":"` + requestID + `","LogicalResourceId":"ArchiveMode","StackId":"` + stackID +
		`","PhysicalResourceId":"` + table + `/archive-mode"}`
	if got := r.all(); len(got) != 2 || got[0].body != reply || got[1].body != reply {
		t.Fatalf("replies %+v\nwant two of\n%s\nstderr:\n%s", got, reply, &c.stderr)
	}
	s, _, err := meta.New(dynamo.New(dynamodb.NewFromConfig(cfg)), table).Stream(t.Context(), "clicks")
	if err != nil || s.Mode != meta.ArchiveOff || len(s.Holes) != 1 || s.Holes[0].From != meta.ArchiveAll || s.Holes[0].To != meta.ArchiveOff {
		t.Errorf("stream item %+v, %v; want mode off after a hole from all to off", s, err)
	}
}
