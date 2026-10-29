// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine_test

import (
	"bufio"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/engine"
)

// Every token of testdata/compat/v1 names the receipts that its events' receipts are stored
// at, so a release that reads token format 1 otherwise, even one that writes it otherwise
// too, fails here (ADR 0019). The expected values were decoded apart from this code.
func TestCompatTokenFormat1(t *testing.T) {
	t.Parallel()
	type decoded struct {
		region, stream string
		events         []engine.TokenEvent
	}
	want := map[string]decoded{
		"one-event": {"local", "page_views", []engine.TokenEvent{
			{EventID: "00VHNCZB00V63QXKKD6T5AR6KF", IngestMicros: 946684800000000, Flags: 0},
		}},
		"batch": {"local", "api_calls", []engine.TokenEvent{
			{EventID: "5VASCAJ90WHA7EFP4QXZPV8N5G", IngestMicros: 946684800000002, Flags: 0},
			{EventID: "1W65EZ2EZN55V3N4ADGMFCN6Q5", IngestMicros: 946684800000001, Flags: 0},
		}},
		"waitable": {"local", "page_views", []engine.TokenEvent{
			{EventID: "00VHNCZB00V63QXKKD6T5AR6KG", IngestMicros: 946684800000003, Flags: 1},
		}},
		// Each ingest_time is a delta from the event before, not from the first.
		"batch-unordered": {"local", "api_calls", []engine.TokenEvent{
			{EventID: "4BK9F9G921NGDNN6S4VNGCDYH7", IngestMicros: 946684800000001, Flags: 1},
			{EventID: "6FHA2KH8PTSPM3JDW4D68JSPP9", IngestMicros: 946684800000000, Flags: 0},
			{EventID: "2F9VE076JANPB35G0SVTAP9BA5", IngestMicros: 946684800000001, Flags: 1},
		}},
	}
	f, err := os.Open("../../testdata/compat/v1/tokens.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seen := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, text, ok := strings.Cut(sc.Text(), " ")
		if !ok || strings.HasPrefix(name, "#") {
			continue
		}
		seen++
		var got decoded
		got.region, got.stream, got.events, err = engine.DecodeToken(text)
		w := want[name]
		if err != nil || got.region != w.region || got.stream != w.stream || !slices.Equal(got.events, w.events) {
			t.Errorf("%s decodes to %+v, %v; want %+v", name, got, err, w)
		}
	}
	if err = sc.Err(); err != nil || seen != len(want) {
		t.Errorf("tokens.txt holds %d tokens (%v), want %d", seen, err, len(want))
	}
}
