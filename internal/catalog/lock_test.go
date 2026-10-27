// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
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

// defR is defB rebuilt once.
func defR() catalog.ViewVersion {
	v := defB()
	v.Rebuild = 1
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

// summary lists the versions in l as "view@vN state definition mode", in name and number
// order, naming each definition by its letter. A retired version has no mode.
func summary(t *testing.T, l *catalog.Lock) []string {
	t.Helper()
	letters := map[string]string{}
	for letter, def := range map[string]catalog.ViewVersion{"A": defA(), "B": defB(), "C": defC(), "R": defR(), "T": asDef(tagHourly())} {
		letters[locked(t, def, 1, catalog.StateCurrent).Fingerprint] = letter
	}
	var out []string
	for _, name := range slices.Sorted(maps.Keys(l.Views)) {
		vs := slices.Clone(l.Views[name])
		slices.SortFunc(vs, func(a, b catalog.ViewVersion) int { return a.Number - b.Number })
		for _, vv := range vs {
			out = append(out, strings.TrimSpace(fmt.Sprintf("%s %s %s %s",
				catalog.VersionID(name, vv.Number), vv.State, letters[vv.Fingerprint], vv.Mode)))
		}
	}
	return out
}

// retired returns def as the lock holds it once retired.
func retired(t *testing.T, def catalog.ViewVersion, number int) catalog.ViewVersion {
	t.Helper()
	return catalog.ViewVersion{Number: number, State: catalog.StateRetired, Fingerprint: fingerprinted(t, def).Fingerprint}
}

func TestLockApply(t *testing.T) {
	t.Parallel()
	const (
		cur = catalog.StateCurrent
		ret = catalog.StateRetained
		rtd = catalog.StateRetired
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
			"definition of a retired version",
			[]entry{{defB(), 2, cur}, {defA(), 1, rtd}},
			[]catalog.ViewVersion{defA()},
			[]string{"page_daily@v1 retired A", "page_daily@v2 retained B async", "page_daily@v3 current A async"},
			[]string{"page_daily@v2: retained", "page_daily@v3: new"},
		},
		{
			"retired versions stay retired",
			[]entry{{defC(), 3, ret}, {defB(), 2, cur}, {defA(), 1, rtd}},
			nil,
			[]string{"page_daily@v1 retired A", "page_daily@v2 retained B async", "page_daily@v3 retained C async"},
			[]string{"page_daily@v2: retained"},
		},
		{
			// A retired number is never reused, even when it was the highest.
			"new definition after the highest number retired",
			[]entry{{defB(), 2, rtd}, {defA(), 1, cur}},
			[]catalog.ViewVersion{defC()},
			[]string{"page_daily@v1 retained A async", "page_daily@v2 retired B", "page_daily@v3 current C async"},
			[]string{"page_daily@v1: retained", "page_daily@v3: new"},
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
				vv := locked(t, e.def, e.number, e.state)
				if e.state == rtd {
					vv = retired(t, e.def, e.number)
				}
				l.Views["page_daily"] = append(l.Views["page_daily"], vv)
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

// rebuiltLock holds page_daily v1 retired, v2 retained and v3 current, which rebuilt v2's
// definition, and tag_hourly v1 current.
func rebuiltLock(t *testing.T) *catalog.Lock {
	t.Helper()
	return &catalog.Lock{
		Views: map[string][]catalog.ViewVersion{
			"tag_hourly": {locked(t, asDef(tagHourly()), 1, catalog.StateCurrent)},
			"page_daily": {
				retired(t, defA(), 1), locked(t, defB(), 2, catalog.StateRetained), locked(t, defR(), 3, catalog.StateCurrent),
			},
		},
		Rebuilds: map[string]int{"page_daily": 1},
	}
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

// The golden lock shows the rebuild counts first and a retired entry with only its number,
// state and fingerprint. Regenerate it with go test -update.
func TestMarshalLockGolden(t *testing.T) {
	t.Parallel()
	const path = "testdata/dynaflux.lock"
	got := marshalLock(t, rebuiltLock(t))
	if *update {
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("MarshalLock output differs from %s (go test -update rewrites it):\n%s", path, got)
	}
	l, err := catalog.ParseLock(want)
	if err != nil {
		t.Fatal(err)
	}
	if again := marshalLock(t, l); !bytes.Equal(again, want) {
		t.Errorf("the lock changed in a round trip:\n%s", again)
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
		{"unknown state", edit(`"state": "retained"`, `"state": "deleted"`), `unknown version state "deleted"`},
		{
			"retired entry with a definition",
			edit(`"state": "retained"`, `"state": "retired"`),
			`view page_daily, entry 2: a retired entry holds only its number, state and fingerprint: json: unknown field "mode"`,
		},
		{
			"retired entry with a short fingerprint",
			`{"format": 1, "views": {"page_daily": [{"number": 1, "state": "retired", "fingerprint": "0a"}]}}`,
			`page_daily@v1: fingerprint "0a" isn't 64 lowercase hex digits`,
		},
		{
			"retired entry with an uppercase fingerprint",
			`{"format": 1, "views": {"page_daily": [{"number": 1, "state": "retired", "fingerprint": "` +
				strings.Repeat("A", 64) + `"}]}}`,
			`isn't 64 lowercase hex digits`,
		},
		{
			"retired entry numbered 0",
			`{"format": 1, "views": {"page_daily": [{"number": 0, "state": "retired", "fingerprint": "` +
				strings.Repeat("a", 64) + `"}]}}`,
			"page_daily@v0: version numbers start at 1",
		},
		{"rebuild of an unknown view", edit(`"format": 1,`, `"format": 1, "rebuild": {"page_hourly": 1},`),
			"rebuild names view page_hourly, which the lock doesn't hold"},
		{"rebuild count 0", edit(`"format": 1,`, `"format": 1, "rebuild": {"page_daily": 0},`),
			"view page_daily has rebuild count 0; it must be at least 1"},
		{"rebuild count not an integer", edit(`"format": 1,`, `"format": 1, "rebuild": {"page_daily": 1.5},`),
			"cannot unmarshal number 1.5"},
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

// A rebuilt definition differs from the version it rebuilt only in its rebuild count, so
// it has its own fingerprint and number.
func TestLockApplyRebuilt(t *testing.T) {
	t.Parallel()
	l := &catalog.Lock{Views: map[string][]catalog.ViewVersion{"page_daily": {locked(t, defB(), 1, catalog.StateCurrent)}}}
	next, changes := l.Apply(map[string]catalog.ViewVersion{"page_daily": fingerprinted(t, defR())})
	if got, want := fmt.Sprint(changes), "[page_daily@v1: retained page_daily@v2: new]"; got != want {
		t.Errorf("changes %s, want %s", got, want)
	}
	if got, want := summary(t, next), []string{"page_daily@v1 retained B async", "page_daily@v2 current R async"}; !slices.Equal(got, want) {
		t.Errorf("lock %q, want %q", got, want)
	}
}

func TestApplyKeepsRebuildCounts(t *testing.T) {
	t.Parallel()
	next, _ := rebuiltLock(t).Apply(nil)
	if got := next.Rebuilds; !maps.Equal(got, map[string]int{"page_daily": 1}) {
		t.Errorf("rebuild counts %v, want page_daily 1", got)
	}
}

// A lock that lost its rebuild counts, as a bad merge can leave it, is rejected: the next
// definition would get another fingerprint than the current version.
func TestParseLockRejectsLostRebuildCount(t *testing.T) {
	t.Parallel()
	const counts = "  \"rebuild\": {\n    \"page_daily\": 1\n  },\n"
	data := string(marshalLock(t, rebuiltLock(t)))
	if !strings.Contains(data, counts) {
		t.Fatalf("the lock has no %q", counts)
	}
	_, err := catalog.ParseLock([]byte(strings.Replace(data, counts, "", 1)))
	if want := "page_daily@v3 has rebuild count 1, more than its view's 0"; err == nil || err.Error() != want {
		t.Errorf("ParseLock = %v, want %q", err, want)
	}
}

func TestCatalogViewsLeaveRetiredOut(t *testing.T) {
	t.Parallel()
	l := rebuiltLock(t)
	l.Views["page_hourly"] = []catalog.ViewVersion{retired(t, defA(), 1)}
	var versions []string
	for _, v := range l.CatalogViews() {
		for _, vv := range v.Versions {
			versions = append(versions, catalog.VersionID(v.Name, vv.Number))
		}
	}
	if want := []string{"page_daily@v2", "page_daily@v3", "tag_hourly@v1"}; !slices.Equal(versions, want) {
		t.Errorf("catalog versions %q, want %q", versions, want)
	}
	if got, want := l.Retired(), []string{"page_daily@v1", "page_hourly@v1"}; !slices.Equal(got, want) {
		t.Errorf("Retired() = %q, want %q", got, want)
	}
}

func TestLockRetire(t *testing.T) {
	t.Parallel()
	l := rebuiltLock(t)
	for _, n := range []int{2, 2, 1} { // retiring twice, and retiring a retired version, changes nothing more
		if err := l.Retire("page_daily", n); err != nil {
			t.Fatalf("Retire(page_daily, %d): %v", n, err)
		}
	}
	want := []string{"page_daily@v1 retired A", "page_daily@v2 retired B", "page_daily@v3 current R async", "tag_hourly@v1 current T async"}
	if got := summary(t, l); !slices.Equal(got, want) {
		t.Errorf("lock %q, want %q", got, want)
	}
	data := string(marshalLock(t, l))
	slim := "{\n        \"number\": 2,\n        \"state\": \"retired\",\n        \"fingerprint\": \"" +
		l.Views["page_daily"][1].Fingerprint + "\"\n      }"
	if !strings.Contains(data, slim) {
		t.Errorf("the lock doesn't hold the slim entry\n%s\n%s", slim, data)
	}
	for _, tt := range []struct {
		view    string
		number  int
		wantErr string
	}{
		{"page_daily", 3, "retire page_daily@v3: it is the current version"},
		{"page_daily", 4, "retire page_daily@v4: dynaflux.lock doesn't hold it"},
		{"page_hourly", 1, "retire page_hourly@v1: dynaflux.lock doesn't hold it"},
	} {
		if err := l.Retire(tt.view, tt.number); err == nil || err.Error() != tt.wantErr {
			t.Errorf("Retire(%s, %d) = %v, want %q", tt.view, tt.number, err, tt.wantErr)
		}
	}
}
