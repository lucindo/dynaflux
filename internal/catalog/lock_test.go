// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Three definitions of page_daily, without a number, state or fingerprint.
func defA() catalog.ViewVersion { return asDef(pageDailyV1()) }
func defB() catalog.ViewVersion { return asDef(pageDailyV2()) }

func defC() catalog.ViewVersion {
	v := defB()
	v.Split = 2
	return v
}

func asDef(v catalog.ViewVersion) catalog.ViewVersion {
	v.Number, v.State, v.Fingerprint = 0, "", ""
	return v
}

// fingerprinted returns def with its fingerprint, as the compiler hands it to Apply.
func fingerprinted(t *testing.T, def catalog.ViewVersion) catalog.ViewVersion {
	t.Helper()
	fp, err := catalog.Fingerprint(&def)
	if err != nil {
		t.Fatal(err)
	}
	def.Fingerprint = fp
	return def
}

// locked returns def as the lock holds it.
func locked(t *testing.T, def catalog.ViewVersion, number int, state catalog.VersionState) catalog.ViewVersion {
	t.Helper()
	def.Number, def.State = number, state
	return fingerprinted(t, def)
}

// entry is a version in a lock.
type entry struct {
	def    catalog.ViewVersion
	number int
	state  catalog.VersionState
}

// summary lists the catalog views of l as "view@vN state definition mode", naming each
// definition by its letter.
func summary(t *testing.T, l *catalog.Lock) []string {
	t.Helper()
	letters := map[string]string{}
	for letter, def := range map[string]catalog.ViewVersion{"A": defA(), "B": defB(), "C": defC(), "T": asDef(tagHourly())} {
		letters[locked(t, def, 1, catalog.StateCurrent).Fingerprint] = letter
	}
	var out []string
	for _, v := range l.CatalogViews() {
		for _, vv := range v.Versions {
			out = append(out, fmt.Sprintf("%s %s %s %s",
				catalog.VersionID(v.Name, vv.Number), vv.State, letters[vv.Fingerprint], vv.Mode))
		}
	}
	return out
}

func TestLockApply(t *testing.T) {
	t.Parallel()
	const (
		cur = catalog.StateCurrent
		ret = catalog.StateRetained
	)
	syncA := defA()
	syncA.Mode = catalog.ModeSync
	tests := []struct {
		name        string
		lock        []entry               // page_daily's versions
		sql         []catalog.ViewVersion // page_daily's definition, if the SQL has one
		want        []string
		wantChanges []string
	}{
		{
			"no lock", nil, []catalog.ViewVersion{defA()},
			[]string{"page_daily@v1 current A async"}, []string{"page_daily@v1: new"},
		},
		{
			"unchanged",
			[]entry{{defA(), 1, cur}},
			[]catalog.ViewVersion{defA()},
			[]string{"page_daily@v1 current A async"}, nil,
		},
		{
			"changed definition",
			[]entry{{defA(), 1, cur}},
			[]catalog.ViewVersion{defB()},
			[]string{"page_daily@v1 retained A async", "page_daily@v2 current B async"},
			[]string{"page_daily@v1: retained", "page_daily@v2: new"},
		},
		{
			"revert to a retained version",
			[]entry{{defB(), 2, cur}, {defA(), 1, ret}},
			[]catalog.ViewVersion{defA()},
			[]string{"page_daily@v1 current A async", "page_daily@v2 retained B async"},
			[]string{"page_daily@v1: current again", "page_daily@v2: retained"},
		},
		{
			"only MODE changed",
			[]entry{{defA(), 1, cur}},
			[]catalog.ViewVersion{syncA},
			[]string{"page_daily@v1 current A sync"}, nil,
		},
		{
			"view removed from the SQL",
			[]entry{{defB(), 2, cur}, {defA(), 1, ret}},
			nil,
			[]string{"page_daily@v1 retained A async", "page_daily@v2 retained B async"},
			[]string{"page_daily@v2: retained"},
		},
		{
			// Branch A added v3 while branch B reverted to v1, and git merged both cleanly.
			"two current versions after a merge",
			[]entry{{defC(), 3, cur}, {defB(), 2, ret}, {defA(), 1, cur}},
			[]catalog.ViewVersion{defA()},
			[]string{"page_daily@v1 current A async", "page_daily@v2 retained B async", "page_daily@v3 retained C async"},
			[]string{"page_daily@v3: retained"},
		},
		{
			// Numbers below the highest are never reused, even when the lock lacks them.
			"new definition after a gap",
			[]entry{{defB(), 5, cur}},
			[]catalog.ViewVersion{defA()},
			[]string{"page_daily@v5 retained B async", "page_daily@v6 current A async"},
			[]string{"page_daily@v5: retained", "page_daily@v6: new"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := &catalog.Lock{Views: map[string][]catalog.ViewVersion{}}
			for _, e := range tt.lock {
				l.Views["page_daily"] = append(l.Views["page_daily"], locked(t, e.def, e.number, e.state))
			}
			defs := map[string]catalog.ViewVersion{}
			for _, def := range tt.sql {
				defs["page_daily"] = fingerprinted(t, def)
			}
			next, changes := l.Apply(defs)
			if got := summary(t, next); !slices.Equal(got, tt.want) {
				t.Errorf("lock:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
			var gotChanges []string
			for _, c := range changes {
				gotChanges = append(gotChanges, c.String())
			}
			if !slices.Equal(gotChanges, tt.wantChanges) {
				t.Errorf("changes %q, want %q", gotChanges, tt.wantChanges)
			}
		})
	}
}

func TestLockApplyLeavesTheLockUnchanged(t *testing.T) {
	t.Parallel()
	l := &catalog.Lock{Views: map[string][]catalog.ViewVersion{
		"page_daily": {locked(t, defA(), 1, catalog.StateCurrent)},
	}}
	l.Apply(map[string]catalog.ViewVersion{"page_daily": fingerprinted(t, defB())})
	if got := summary(t, l); !slices.Equal(got, []string{"page_daily@v1 current A async"}) {
		t.Errorf("lock after Apply: %q", got)
	}
}

// sampleLock holds page_daily v1 retained and v2 current, and tag_hourly v1 current.
func sampleLock(t *testing.T) *catalog.Lock {
	t.Helper()
	return &catalog.Lock{Views: map[string][]catalog.ViewVersion{
		"tag_hourly": {locked(t, asDef(tagHourly()), 1, catalog.StateCurrent)},
		"page_daily": {locked(t, defA(), 1, catalog.StateRetained), locked(t, defB(), 2, catalog.StateCurrent)},
	}}
}

func marshalLock(t *testing.T, l *catalog.Lock) []byte {
	t.Helper()
	data, err := catalog.MarshalLock(l)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestMarshalLockLayout(t *testing.T) {
	t.Parallel()
	data := string(marshalLock(t, sampleLock(t)))
	// Views by name, versions newest first, each entry headed by its number, state,
	// fingerprint and mode.
	const head = "{\n  \"format\": 1,\n  \"views\": {\n    \"page_daily\": [\n      {\n        \"number\": 2,\n" +
		"        \"state\": \"current\",\n        \"fingerprint\": \""
	if !strings.HasPrefix(data, head) {
		t.Errorf("lock starts:\n%.300s\nwant:\n%s", data, head)
	}
	v1 := strings.Index(data, "\"number\": 1,\n        \"state\": \"retained\"")
	tags := strings.Index(data, "\"tag_hourly\": [")
	if v1 < 0 || tags < 0 || v1 > tags || !strings.HasSuffix(data, "\n  }\n}\n") {
		t.Errorf("unexpected layout:\n%s", data)
	}
}

func TestMarshalLockEmpty(t *testing.T) {
	t.Parallel()
	const want = "{\n  \"format\": 1,\n  \"views\": {}\n}\n"
	if got := string(marshalLock(t, &catalog.Lock{})); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestLockRoundTrip(t *testing.T) {
	t.Parallel()
	data := marshalLock(t, sampleLock(t))
	l, err := catalog.ParseLock(data)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"page_daily@v1 retained A async", "page_daily@v2 current B async", "tag_hourly@v1 current T async"}
	if got := summary(t, l); !slices.Equal(got, want) {
		t.Errorf("parsed lock %q, want %q", got, want)
	}
	if again := marshalLock(t, l); !bytes.Equal(again, data) {
		t.Errorf("the lock changed in a round trip:\n%s\nwant:\n%s", again, data)
	}
}

func TestParseLockAcceptsAnyLayout(t *testing.T) {
	t.Parallel()
	var compact bytes.Buffer
	if err := json.Compact(&compact, marshalLock(t, sampleLock(t))); err != nil {
		t.Fatal(err)
	}
	l, err := catalog.ParseLock(compact.Bytes())
	if err != nil {
		t.Fatalf("ParseLock rejected a compact lock: %v", err)
	}
	want := []string{"page_daily@v1 retained A async", "page_daily@v2 current B async", "tag_hourly@v1 current T async"}
	if got := summary(t, l); !slices.Equal(got, want) {
		t.Errorf("parsed lock %q, want %q", got, want)
	}
}

func TestParseLockRejects(t *testing.T) {
	t.Parallel()
	valid := string(marshalLock(t, sampleLock(t)))
	edit := func(old, repl string) string {
		if !strings.Contains(valid, old) {
			t.Fatalf("the lock has no %q", old)
		}
		return strings.Replace(valid, old, repl, 1)
	}
	tests := []struct{ name, data, want string }{
		{
			"merge-conflict markers",
			edit("    \"tag_hourly\"", "<<<<<<< HEAD\n    \"tag_hourly\""),
			"invalid JSON: invalid character '<' looking for beginning of object key string",
		},
		{"trailing data", valid + "{}", "invalid JSON: invalid character '{' after top-level value"},
		{"newer format", edit(`"format": 1`, `"format": 2`), "format 2 is not supported; this Dynaflux reads format 1"},
		{"no format", edit(`"format": 1,`, ""), "format 0 is not supported"},
		{"unknown field", edit(`"format": 1,`, `"format": 1, "comment": "x",`), `json: unknown field "comment"`},
		{
			"unknown field in an entry",
			edit(`"number": 2,`, `"number": 2, "n": 2,`),
			`view page_daily, entry 1: json: unknown field "n"`,
		},
		{"retired entry", edit(`"state": "retained"`, `"state": "retired"`), `unknown version state "retired"`},
		{"no state", edit(`"state": "retained",`, ""), "page_daily@v1 has no state"},
		{"no mode", edit(`"mode": "async",`, ""), "page_daily@v2 has no mode"},
		{
			"hand edit",
			edit(`"split": 1`, `"split": 3`),
			"page_daily@v2: fingerprint \"" + sampleLock(t).Views["page_daily"][1].Fingerprint +
				"\" doesn't match the definition",
		},
		{"repeated number", edit(`"number": 2,`, `"number": 1,`), "view page_daily: version 1 appears twice"},
		{"number below 1", edit(`"number": 2,`, `"number": 0,`), "page_daily@v0: version numbers start at 1"},
		{"view without versions", `{"format": 1, "views": {"page_daily": []}}`, "view page_daily has no versions"},
		{"invalid view name", `{"format": 1, "views": {"Page": []}}`, `key "views.Page" isn't a lower-case name`},
		{
			// A merge tool's "accept both" when two branches add the same view.
			"repeated view",
			edit(`"tag_hourly": [`, `"page_daily": [`),
			`key "views.page_daily" appears twice`,
		},
		{
			"repeated field",
			edit(`"state": "current",`, `"state": "retained", "state": "current",`),
			`key "views.page_daily[0].state" appears twice`,
		},
		{"field in capitals", edit(`"number": 2,`, `"NUMBER": 2,`), `key "views.page_daily[0].NUMBER" isn't a lower-case name`},
		{"format in capitals", edit(`"format": 1`, `"FORMAT": 1`), `key "FORMAT" isn't a lower-case name`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l, err := catalog.ParseLock([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ParseLock = %v, %v; want an error containing %q", l, err, tt.want)
			}
		})
	}
}

func TestParseLockLocatesSyntaxErrors(t *testing.T) {
	t.Parallel()
	data := "{\n  \"format\": 1,\n<<<<<<< HEAD\n}\n"
	_, err := catalog.ParseLock([]byte(data))
	var se *json.SyntaxError
	if !errors.As(err, &se) {
		t.Fatalf("ParseLock = %v, want a *json.SyntaxError", err)
	}
	if want := int64(strings.Index(data, "<") + 1); se.Offset != want {
		t.Errorf("offset %d, want %d", se.Offset, want)
	}
}
