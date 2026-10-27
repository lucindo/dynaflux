// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

const (
	// ingestWarning is what an import into page_views warns about page_hourly.
	ingestWarning = "us-east-1: warning DF7404: page_hourly@v1 groups by ingest_time, so it counts the imported events at the time of the import\n" +
		"  hint: a version that groups by event_time counts them at their own time\n"
	viewsPlan = "plan: import 3 events of input/views.jsonl into page_views in us-east-1, at most 500 a second; an import can't be undone\n"
)

// importedViews returns the events of input/views.jsonl as an import at the bubble's start
// stores them.
func importedViews() []string {
	return []string{
		`{"user_id":"u1","page":"/home","duration_ms":120,"tags":[],"referrer":null,"event_time":"2024-05-31T13:45:00.250000Z","ingest_time":"2000-01-01T00:00:00.000000Z","origin_region":"us-east-1"}`,
		`{"user_id":"u1","page":"/home","duration_ms":7,"tags":[],"referrer":null,"event_time":"2024-06-01T00:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.004000Z","origin_region":"us-east-1"}`,
		`{"user_id":"u2","page":"/cart","duration_ms":80,"tags":["a","b"],"referrer":"https://example.com/?a=1&b=2","event_time":"2024-05-31T12:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.002000Z","origin_region":"us-east-1"}`,
	}
}

// importStore is testdata/import.txtar's store in memory, its catalog registered in
// us-east-1.
type importStore struct {
	dir string
	b   store.Backend
	cat *engine.Catalog
}

func newImportStore(t *testing.T) *importStore {
	t.Helper()
	s := &importStore{dir: writeProject(t, "import"), b: mem.New()}
	data, err := catalog.Marshal(compile(t, s.dir).Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = meta.New(s.b, "shop-meta").RegisterCatalog(t.Context(), data, "v0.5.0"); err != nil {
		t.Fatal(err)
	}
	if s.cat, err = engine.Compile(data); err != nil {
		t.Fatal(err)
	}
	return s
}

// importRun is one run of dynaflux import.
type importRun struct {
	args       []string
	stdin      string
	terminal   bool
	clockAhead time.Duration
	ctx        context.Context // nil for the test's
	b          store.Backend   // nil for the store's
}

func (s *importStore) run(t *testing.T, r importRun) result {
	t.Helper()
	ctx, b := r.ctx, r.b
	if ctx == nil {
		ctx = t.Context()
	}
	if b == nil {
		b = s.b
	}
	var stdout, stderr strings.Builder
	status := cli.RunEnv(ctx, slices.Concat([]string{"import", "-region", "us-east-1"}, r.args), strings.NewReader(r.stdin), &stdout, &stderr, s.dir, cli.Env{
		Terminal: r.terminal, Account: account, ClockAhead: r.clockAhead,
		Backends: map[string]store.Backend{"us-east-1": b},
	})
	return result{status, stdout.String(), stderr.String()}
}

// stored returns the events of stream as JSON objects without event_id, sorted.
func (s *importStore) stored(t *testing.T, stream string) []string {
	t.Helper()
	ps, _ := s.cat.Projector().Stream(stream)
	var cols []catalog.Column
	for _, col := range ps.Columns {
		cols = append(cols, catalog.Column{Name: col.Name, Type: col.Type})
	}
	n := len(cols)
	cols = append(cols, catalog.ImplicitColumns()[1:]...) // without event_id
	var events []string
	var start *store.Key
	for {
		items, next, err := s.b.ScanPage(t.Context(), catalog.Naming{Store: "shop"}.StreamTable(stream), store.Scan{Segments: 1}, start)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			vals, bad := ps.Decode(it)
			if bad != nil {
				t.Fatalf("stored event doesn't decode: %v", bad)
			}
			events = append(events, jsonObject(cols, slices.Delete(vals, n, n+1)))
		}
		if next == nil {
			slices.Sort(events)
			return events
		}
		start = next
	}
}

// jsonObject returns the JSON object of vals, the values of cols.
func jsonObject(cols []catalog.Column, vals []expr.Value) string {
	line := []byte{'{'}
	for i, col := range cols {
		if i > 0 {
			line = append(line, ',')
		}
		line = fmt.Appendf(line, "%q:", col.Name)
		line = valuetext.AppendJSON(line, col.Type, vals[i])
	}
	return string(append(line, '}'))
}

// writeInput writes a file of the store's project.
func (s *importStore) writeInput(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(s.dir, name), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func (s *importStore) readInput(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// importCase is a case of TestImport.
type importCase struct {
	name string
	run  importRun
	// before prepares the store and the project.
	before func(t *testing.T, s *importStore)
	want   result
	// stored are the events of stream after the run, when stream isn't "".
	stream string
	stored []string
}

// check runs tt on a new store.
func (tt importCase) check(t *testing.T) {
	t.Helper()
	s := newImportStore(t)
	if tt.before != nil {
		tt.before(t, s)
	}
	r := tt.run
	if slices.Contains(r.args, "-") {
		r.stdin = string(s.readInput(t, "input/views.jsonl"))
	}
	if got := s.run(t, r); got != tt.want {
		t.Errorf("got  %+v\nwant %+v", got, tt.want)
	}
	if tt.stream == "" {
		return
	}
	if got := s.stored(t, tt.stream); !slices.Equal(got, tt.stored) {
		t.Errorf("stored:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(tt.stored, "\n"))
	}
}

func TestImport(t *testing.T) {
	t.Parallel()
	views := []string{"page_views", "-file", "input/views.jsonl", "-yes"}
	payments := []string{"payments", "-file", "input/payments.jsonl", "-yes"}
	tests := []importCase{
		{
			name: "import", run: importRun{args: views},
			want:   result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning + viewsPlan},
			stream: "page_views", stored: importedViews(),
		},
		{
			name: "check", run: importRun{args: []string{"page_views", "-file", "input/views.jsonl", "-check"}},
			want:   result{0, "input/views.jsonl: 3 events fit page_views in us-east-1\n", header + ingestWarning},
			stream: "page_views",
		},
		{
			name: "every type", run: importRun{args: []string{"typed", "-file", "input/typed.jsonl", "-yes"}},
			want: result{0, "2 events: 2 stored, 0 already present, 0 conflicting\n",
				header + "plan: import 2 events of input/typed.jsonl into typed in us-east-1, at most 500 a second; an import can't be undone\n"},
			stream: "typed", stored: []string{
				`{"id":"max","u8":255,"u16":65535,"u32":4294967295,"u64":"18446744073709551615","i8":-128,"i16":-32768,"i32":-2147483648,"i64":"-9223372036854775808","f":1.5e+100,"d":"-99999999.99","b":true,"u":"61f0c404-5cb3-11e7-907b-a6006ad3dba0","day":"2106-02-07","ms":"2024-05-31T13:45:00.123Z","tags":["x","it's"],"nums":[1,-2],"note":"ok","event_time":"2024-05-31T13:45:00.123456Z","ingest_time":"2000-01-01T00:00:00.000000Z","origin_region":"us-east-1"}`,
				`{"id":"min","u8":0,"u16":0,"u32":0,"u64":"0","i8":0,"i16":0,"i32":0,"i64":"9007199254740993","f":-0.25,"d":"0.10","b":false,"u":"00000000-0000-0000-0000-000000000000","day":"1970-01-01","ms":"1970-01-01T00:00:00.000Z","tags":[],"nums":[],"note":null,"event_time":"1970-01-01T00:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.002000Z","origin_region":"us-east-1"}`,
			},
		},
		{
			name: "invalid lines", run: importRun{args: []string{"page_views", "-file", "input/bad.jsonl", "-yes"}},
			want: result{1, "", header +
				"input/bad.jsonl:2: error DF7401: want a JSON object\n" +
				"input/bad.jsonl:3: error DF7401: invalid JSON: unexpected EOF\n" +
				"input/bad.jsonl:4: error DF7402: page_views has no column pag in us-east-1; did you mean page?\n" +
				"input/bad.jsonl:5: error DF7402: event_id is set by the import; leave it out\n" +
				"input/bad.jsonl:6: error DF7401: user_id appears twice\n" +
				"input/bad.jsonl:7: error DF7401: page is missing; it is neither Nullable nor has a DEFAULT\n" +
				"input/bad.jsonl:8: error DF7401: event_time is missing; import needs each event's time\n" +
				"input/bad.jsonl:9: error DF7401: event_time is null; import needs each event's time\n" +
				"input/bad.jsonl:10: error DF7401: duration_ms: invalid UInt32: want an integer\n" +
				"input/bad.jsonl:11: error DF7401: event_time: invalid DateTime64(6): want RFC 3339 with at most 6 fractional digits, such as 2026-10-26T13:45:00.250Z\n" +
				"input/bad.jsonl:12: error DF7401: more than one JSON value on the line\n" +
				"input/bad.jsonl:13: error DF7402: page_views has no column pa\\x1b[31mge in us-east-1\n" +
				"dynaflux import: 12 of 13 events can't be imported; nothing written\n"},
			stream: "page_views",
		},
		{
			name: "invalid events", run: importRun{args: []string{"page_views", "-file", "input/generated.jsonl", "-yes"}},
			before: func(t *testing.T, s *importStore) {
				t.Helper()
				line := func(user, page, tags string) string {
					return fmt.Sprintf(`{"user_id":%q,"page":%q,"duration_ms":1,"tags":[%s],"event_time":"2024-05-31T13:45:00Z"}`+"\n", user, page, tags)
				}
				s.writeInput(t, "input/generated.jsonl", slices.Concat(
					[]byte(`{"user_id":"u1","page":"`), []byte{0xff}, []byte(`","duration_ms":1,"event_time":"2024-05-31T13:45:00Z"}`+"\n"),
					[]byte(line(strings.Repeat("u", 2049), "/", "")),
					[]byte(line("u1", "/", strings.Repeat(`"t",`, 1000)+`"t"`)),
					[]byte(line("u1", strings.Repeat("p", 400<<10), "")),
				))
			},
			want: result{1, "", header +
				"input/generated.jsonl:1: error DF7401: the line isn't valid UTF-8\n" +
				"input/generated.jsonl:2: error DF7401: user_id: invalid value: the partition key is 2049 bytes, over 2048\n" +
				"input/generated.jsonl:3: error DF7401: tags: invalid value: 1001 elements, over 1000\n" +
				"input/generated.jsonl:4: error DF7401: invalid value: the event is over DynamoDB's 400 KB item limit\n" +
				"dynaflux import: 4 of 4 events can't be imported; nothing written\n"},
			stream: "page_views",
		},
		{
			name: "gzip", run: importRun{args: []string{"page_views", "-file", "input/views.jsonl.gz", "-yes"}},
			before: func(t *testing.T, s *importStore) {
				t.Helper()
				var b bytes.Buffer
				zw := gzip.NewWriter(&b)
				if _, err := zw.Write(s.readInput(t, "input/views.jsonl")); err != nil {
					t.Fatal(err)
				}
				if err := zw.Close(); err != nil {
					t.Fatal(err)
				}
				s.writeInput(t, "input/views.jsonl.gz", b.Bytes())
			},
			want: result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
				"plan: import 3 events of input/views.jsonl.gz into page_views in us-east-1, at most 500 a second; an import can't be undone\n"},
			stream: "page_views", stored: importedViews(),
		},
		{
			name: "standard input", run: importRun{args: []string{"page_views", "-file", "-", "-yes"}},
			want: result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
				"plan: import 3 events of stdin into page_views in us-east-1, at most 500 a second; an import can't be undone\n"},
			stream: "page_views", stored: importedViews(),
		},
		{
			name: "standard input asks for -yes", run: importRun{args: []string{"page_views", "-file", "-"}, terminal: true},
			want: result{2, "", header + ingestWarning +
				"plan: import 3 events of stdin into page_views in us-east-1, at most 500 a second; an import can't be undone\n" +
				"dynaflux import: import 3 events into page_views in us-east-1?: confirm with -yes when not on a terminal\n" +
				"Run 'dynaflux help import' for usage.\n"},
			stream: "page_views",
		},
		{
			name: "skip", run: importRun{args: slices.Concat(views, []string{"-skip", "2"})},
			want: result{0, "2 events: 2 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
				"plan: import 2 events of input/views.jsonl into page_views in us-east-1, at most 500 a second; an import can't be undone\n"},
			stream: "page_views", stored: []string{
				`{"user_id":"u1","page":"/home","duration_ms":7,"tags":[],"referrer":null,"event_time":"2024-06-01T00:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.002000Z","origin_region":"us-east-1"}`,
				`{"user_id":"u2","page":"/cart","duration_ms":80,"tags":["a","b"],"referrer":"https://example.com/?a=1&b=2","event_time":"2024-05-31T12:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.000000Z","origin_region":"us-east-1"}`,
			},
		},
		{
			name: "skip every line", run: importRun{args: slices.Concat(views, []string{"-skip", "4"})},
			want:   result{0, "input/views.jsonl: no events after line 4\n", header + ingestWarning},
			stream: "page_views",
		},
		{
			name: "re-run with DEDUP BY", run: importRun{args: []string{"payments", "-file", "input/payments_changed.jsonl", "-yes"}},
			before: func(t *testing.T, s *importStore) {
				t.Helper()
				if got := s.run(t, importRun{args: payments}); got.status != 0 {
					t.Fatalf("first import: %+v", got)
				}
				time.Sleep(time.Second)
			},
			want: result{1, "3 events: 1 stored, 1 already present, 1 conflicting\n", header +
				"plan: import 3 events of input/payments_changed.jsonl into payments in us-east-1, at most 500 a second; an import can't be undone\n" +
				"input/payments_changed.jsonl:2: error DF7403: an event with this line's key is stored with other values\n"},
			stream: "payments", stored: []string{
				`{"account":"acme","payment_id":"61f0c404-5cb3-11e7-907b-a6006ad3dba0","amount":"12.50","event_time":"2024-05-31T10:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.000000Z","origin_region":"us-east-1"}`,
				`{"account":"acme","payment_id":"61f0c404-5cb3-11e7-907b-a6006ad3dba1","amount":"3.00","event_time":"2024-05-31T11:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.002000Z","origin_region":"us-east-1"}`,
				`{"account":"acme","payment_id":"61f0c404-5cb3-11e7-907b-a6006ad3dba2","amount":"5.00","event_time":"2024-05-31T12:00:00.000000Z","ingest_time":"2000-01-01T00:00:01.006000Z","origin_region":"us-east-1"}`,
			},
		},
		{
			name: "re-run without DEDUP BY", run: importRun{args: views},
			before: func(t *testing.T, s *importStore) {
				t.Helper()
				if got := s.run(t, importRun{args: views}); got.status != 0 {
					t.Fatalf("first import: %+v", got)
				}
			},
			want:   result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning + viewsPlan},
			stream: "page_views", stored: []string{
				importedViews()[0],
				`{"user_id":"u1","page":"/home","duration_ms":120,"tags":[],"referrer":null,"event_time":"2024-05-31T13:45:00.250000Z","ingest_time":"2000-01-01T00:00:00.004000Z","origin_region":"us-east-1"}`,
				importedViews()[1],
				`{"user_id":"u1","page":"/home","duration_ms":7,"tags":[],"referrer":null,"event_time":"2024-06-01T00:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.008000Z","origin_region":"us-east-1"}`,
				importedViews()[2],
				`{"user_id":"u2","page":"/cart","duration_ms":80,"tags":["a","b"],"referrer":"https://example.com/?a=1&b=2","event_time":"2024-05-31T12:00:00.000000Z","ingest_time":"2000-01-01T00:00:00.006000Z","origin_region":"us-east-1"}`,
			},
		},
		{
			name: "clock ahead", run: importRun{args: views, clockAhead: 61 * time.Second},
			want: result{1, "", header + ingestWarning +
				"us-east-1: error DF7405: this machine's clock is 1m1s ahead of DynamoDB's, more than 1m0s\n" +
				"  hint: set the clock, with NTP, and run import again\n"},
			stream: "page_views",
		},
		{
			name: "clock behind", run: importRun{args: views, clockAhead: -61 * time.Second},
			want: result{1, "", header + ingestWarning +
				"us-east-1: error DF7405: this machine's clock is 1m1s behind DynamoDB's, more than 1m0s\n" +
				"  hint: set the clock, with NTP, and run import again\n"},
			stream: "page_views",
		},
		{
			name: "clock a minute ahead", run: importRun{args: views, clockAhead: time.Minute},
			want:   result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning + viewsPlan},
			stream: "page_views", stored: importedViews(),
		},
		{
			name: "confirmed", run: importRun{args: views[:3], stdin: "y\n", terminal: true},
			want: result{0, "3 events: 3 stored, 0 already present, 0 conflicting\n", header + ingestWarning + viewsPlan +
				"import 3 events into page_views in us-east-1? [y/N] "},
			stream: "page_views", stored: importedViews(),
		},
		{
			name: "declined", run: importRun{args: views[:3], stdin: "n\n", terminal: true},
			want: result{1, "", header + ingestWarning + viewsPlan +
				"import 3 events into page_views in us-east-1? [y/N] dynaflux import: nothing written\n"},
			stream: "page_views",
		},
		{
			name: "not on a terminal", run: importRun{args: views[:3]},
			want: result{2, "", header + ingestWarning + viewsPlan +
				"dynaflux import: import 3 events into page_views in us-east-1?: confirm with -yes when not on a terminal\n" +
				"Run 'dynaflux help import' for usage.\n"},
			stream: "page_views",
		},
		{
			name: "unknown stream", run: importRun{args: []string{"page_view", "-file", "input/views.jsonl"}},
			want: result{2, "", header + "dynaflux import: the deployed catalog of us-east-1 has no stream page_view; did you mean page_views?\n" +
				"Run 'dynaflux help import' for usage.\n"},
		},
		{
			name: "missing file", run: importRun{args: []string{"page_views", "-file", "input/none.jsonl"}},
			want: result{2, "", header + "dynaflux import: -file input/none.jsonl: no such file or directory\n"},
		},
		{
			name: "no file", run: importRun{args: []string{"page_views"}},
			want: result{2, "", "dynaflux import: name the input with -file, or -file - for standard input\nRun 'dynaflux help import' for usage.\n"},
		},
		{
			name: "no rate", run: importRun{args: slices.Concat(views, []string{"-rate", "0"})},
			want: result{2, "", "dynaflux import: -rate must be 1 or more\nRun 'dynaflux help import' for usage.\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, tt.check)
		})
	}
}

// pageViews writes n events of page_views to input/many.jsonl, one per line.
func (s *importStore) pageViews(t *testing.T, n int) {
	t.Helper()
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, `{"user_id":"u1","page":"/p%d","duration_ms":%d,"event_time":"2024-05-31T13:45:00Z"}`+"\n", i+1, i+1)
	}
	s.writeInput(t, "input/many.jsonl", []byte(b.String()))
}

// Import sends -rate events a second, and reports its progress every 10 seconds.
func TestImportPacing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newImportStore(t)
		s.pageViews(t, 70)
		start := time.Now()
		got := s.run(t, importRun{args: []string{"page_views", "-file", "input/many.jsonl", "-yes", "-rate", "3"}})
		want := result{0, "70 events: 70 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
			"plan: import 70 events of input/many.jsonl into page_views in us-east-1, at most 3 a second; an import can't be undone\n" +
			"through line 31 of 70: 31 stored, 0 already present, 0 conflicting; 3 events a second\n" +
			"through line 61 of 70: 61 stored, 0 already present, 0 conflicting; 3 events a second\n"}
		if got != want {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
		// The 70th event is sent 69 intervals of a third of a second after the first.
		if took := time.Since(start); took != 69*(time.Second/3) {
			t.Errorf("took %v, want %v", took, 69*(time.Second/3))
		}
	})
}

// An interrupt stops the import after the writes under way, and -skip resumes it.
func TestImportInterruptedResumes(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newImportStore(t)
		s.pageViews(t, 70)
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(5*time.Second, cancel)
		args := []string{"page_views", "-file", "input/many.jsonl", "-yes", "-rate", "3"}
		got := s.run(t, importRun{args: args, ctx: ctx})
		want := result{1, "16 events: 16 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
			"plan: import 70 events of input/many.jsonl into page_views in us-east-1, at most 3 a second; an import can't be undone\n" +
			"dynaflux import: interrupted\n  hint: the lines through 16 are done; resume with -skip 16\n"}
		if got != want {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
		got = s.run(t, importRun{args: slices.Concat(args, []string{"-rate", "500", "-skip", "16"})})
		if want := "54 events: 54 stored, 0 already present, 0 conflicting\n"; got.status != 0 || got.stdout != want {
			t.Errorf("resumed: %+v, want status 0 and stdout %q", got, want)
		}
		if n := len(s.stored(t, "page_views")); n != 70 {
			t.Errorf("stored %d events, want 70", n)
		}
	})
}

// failPut fails the writes of the events of one page.
type failPut struct {
	store.Backend

	page string
}

var errAccess = errors.New("AccessDeniedException: not authorized to perform dynamodb:PutItem")

func (f failPut) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if it["page"].Str == f.page {
		return errAccess
	}
	return f.Backend.Put(ctx, table, it, c)
}

// A write that fails stops the import, which names the line to resume after.
func TestImportWriteFails(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newImportStore(t)
		s.pageViews(t, 10)
		got := s.run(t, importRun{args: []string{"page_views", "-file", "input/many.jsonl", "-yes"}, b: failPut{s.b, "/p5"}})
		want := result{2, "4 events: 4 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
			"plan: import 10 events of input/many.jsonl into page_views in us-east-1, at most 500 a second; an import can't be undone\n" +
			"dynaflux import: us-east-1: line 5: append to page_views: " + errAccess.Error() + "\n" +
			"  hint: the lines through 4 are done; resume with -skip 4\n"}
		if got != want {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
	})
}

// slowFailPut fails the write of the events of one page after a delay, while later writes
// land.
type slowFailPut struct {
	store.Backend

	page string
}

func (f slowFailPut) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	if it["page"].Str == f.page {
		// Odd, so the failure never falls on a send, which leaves every 2 ms at the default
		// rate: at one instant of the bubble's clock, either could run first.
		time.Sleep(51 * time.Millisecond)
		return errAccess
	}
	return f.Backend.Put(ctx, table, it, c)
}

// Writes that end after a failed one store lines past the one to resume after; a stream
// without DEDUP BY names them, so the resume stores every event once.
func TestImportWriteFailsSlowly(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := newImportStore(t)
		s.pageViews(t, 40)
		args := []string{"page_views", "-file", "input/many.jsonl", "-yes"}
		got := s.run(t, importRun{args: args, b: slowFailPut{s.b, "/p5"}})
		want := result{2, "29 events: 29 stored, 0 already present, 0 conflicting\n", header + ingestWarning +
			"plan: import 40 events of input/many.jsonl into page_views in us-east-1, at most 500 a second; an import can't be undone\n" +
			"dynaflux import: us-east-1: line 5: append to page_views: " + errAccess.Error() + "\n" +
			"  hint: the lines through 4 are done, and lines 6-30 are stored too; page_views has no DEDUP BY, " +
			"so empty those lines in the input before you resume with -skip 4, or their events are stored twice\n"}
		if got != want {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
		lines := strings.SplitAfter(string(s.readInput(t, "input/many.jsonl")), "\n")
		for i := 6; i <= 30; i++ {
			lines[i-1] = "\n"
		}
		s.writeInput(t, "input/many.jsonl", []byte(strings.Join(lines, "")))
		got = s.run(t, importRun{args: slices.Concat(args, []string{"-skip", "4"})})
		if want := "11 events: 11 stored, 0 already present, 0 conflicting\n"; got.status != 0 || got.stdout != want {
			t.Errorf("resumed: %+v, want status 0 and stdout %q", got, want)
		}
		if n := len(s.stored(t, "page_views")); n != 40 {
			t.Errorf("stored %d events, want 40", n)
		}
	})
}
