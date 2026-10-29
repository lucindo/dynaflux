// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/txtar"
)

const (
	usage = `Dynaflux runs event-sourced fact streams and materialized views on DynamoDB Global Tables.

Usage:
  dynaflux <command> [arguments]

Commands:
  init            start a project: dynaflux.yaml and an example stream, view and query
  validate        check dynaflux.yaml and the SQL definitions
  explain         show the layout and cost of a stream, view or query
  generate (gen)  write the Go client package and templates, and update dynaflux.lock
  diff            compare the project with the deployed store
  dev             run the store on DynamoDB Local for an application under development
  status          show the state of the deployed store, and what needs an action
  backfill        fill view versions in the deployed store, so they go live
  retire          retire a view version that is no longer current
  inspect         read a stream's events or a view's rows in the deployed store
  query           run a named query on the deployed store
  import          load past events from JSON Lines into a stream of the deployed store
  version         show the versions of the CLI, the library and generated code
  help            show help for a command

Run 'dynaflux help <command>' for more about a command.
`
	// The diagnostics of testdata/errors.txtar, with the texts of sql.md appendix A.
	errorsDiagnostics = `dynaflux/views.sql:4:6: error DF3001: page_daily: page_view is not a stream
  hint: did you mean page_views?
dynaflux/views.sql:9:17: warning DF3503: page_referrer: argMax skips rows where referrer is NULL, so NULL never replaces a value
  hint: use a sentinel: argMax(coalesce(referrer, ''), event_time)
`
)

type result struct {
	status         int
	stdout, stderr string
}

func run(dir string, args ...string) result {
	var stdout, stderr strings.Builder
	status := cli.Run(context.Background(), args, strings.NewReader(""), &stdout, &stderr, dir)
	return result{status, stdout.String(), stderr.String()}
}

// runAlone runs dynaflux as run does, but as if no directory above dir held a go.mod.
func runAlone(dir string, args ...string) result {
	var stdout, stderr strings.Builder
	status := cli.RunEnv(context.Background(), args, strings.NewReader(""), &stdout, &stderr, dir, cli.Env{GoModCeiling: dir})
	return result{status, stdout.String(), stderr.String()}
}

// writeProject writes the files of testdata/<name>.txtar, except its want/ sections, into
// a new temporary directory and returns the directory.
func writeProject(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name+".txtar"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, f := range txtar.Parse(data).Files {
		if strings.HasPrefix(f.Name, wantPrefix) {
			continue
		}
		path := filepath.Join(dir, filepath.FromSlash(f.Name))
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
			t.Fatal(mkErr)
		}
		if writeErr := os.WriteFile(path, f.Data, 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	return dir
}

func TestCommands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want result
	}{
		{"no arguments", nil, result{2, "", usage}},
		{"help", []string{"help"}, result{0, usage, ""}},
		{"-h", []string{"-h"}, result{0, usage, ""}},
		{"--help", []string{"--help"}, result{0, usage, ""}},
		{
			"help on a command", []string{"help", "validate"},
			result{0, `Usage: dynaflux validate

Validate finds dynaflux.yaml in the working directory or the nearest parent that has
one, and checks it and every SQL file it matches. It prints each problem on
standard error as

  path:line:col: severity code: message
    hint: hint

with paths relative to the project root.

Exit status: 0 when there are no errors (warnings are allowed), 1 when there is an
error, and 2 for usage and I/O problems.
`, ""},
		},
		{
			"help on two commands", []string{"help", "validate", "explain"},
			result{2, "", "dynaflux help: name at most one command\nRun 'dynaflux help help' for usage.\n"},
		},
		{
			"unknown command", []string{"valdate"},
			result{2, "", "dynaflux: unknown command \"valdate\"; did you mean validate?\nRun 'dynaflux help' for usage.\n"},
		},
		{
			"help on an unknown command", []string{"help", "explian"},
			result{2, "", "dynaflux: unknown command \"explian\"; did you mean explain?\nRun 'dynaflux help' for usage.\n"},
		},
		{
			"alias with an argument", []string{"gen", "x"},
			result{2, "", "dynaflux generate: unexpected arguments: x\nRun 'dynaflux help generate' for usage.\n"},
		},
		{
			"unknown command close to an alias", []string{"gne"},
			result{2, "", "dynaflux: unknown command \"gne\"; did you mean gen?\nRun 'dynaflux help' for usage.\n"},
		},
		{
			"unknown flag", []string{"validate", "--strict"},
			result{2, "", "dynaflux validate: flag provided but not defined: -strict\nRun 'dynaflux help validate' for usage.\n"},
		},
		{
			"version", []string{"version"},
			result{0, "cli             (devel)\nlibrary         (no go.mod)\ngenerated code  version 1\n", ""},
		},
		{
			"version with an argument", []string{"version", "x"},
			result{2, "", "dynaflux version: unexpected arguments: x\nRun 'dynaflux help version' for usage.\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := runAlone(t.TempDir(), tt.args...); got != tt.want {
				t.Errorf("dynaflux %q:\ngot  %+v\nwant %+v", tt.args, got, tt.want)
			}
		})
	}
}

func TestHelpMatchesDashH(t *testing.T) {
	t.Parallel()
	help := run(t.TempDir(), "help", "explain")
	dashH := run(t.TempDir(), "explain", "-h")
	if help != dashH {
		t.Errorf("help explain:\n%+v\nexplain -h:\n%+v", help, dashH)
	}
	for _, want := range []string{"Usage: dynaflux explain [flags] <stream|view|query>\n", "  -rows view=N\n", "(default 365d)"} {
		if !strings.Contains(help.stdout, want) {
			t.Errorf("help explain lacks %q:\n%s", want, help.stdout)
		}
	}
}

// TestHelpWidth checks that each command's help wraps its prose within helpWidth columns.
// An indented line, such as a flag's line or a command to copy, may run longer.
func TestHelpWidth(t *testing.T) {
	t.Parallel()
	const helpWidth = 86
	_, list, _ := strings.Cut(run(t.TempDir(), "help").stdout, "Commands:\n")
	list, _, _ = strings.Cut(list, "\n\n")
	for line := range strings.Lines(list) {
		name := strings.Fields(line)[0]
		for l := range strings.Lines(run(t.TempDir(), "help", name).stdout) {
			if l = strings.TrimSuffix(l, "\n"); len(l) > helpWidth && !strings.HasPrefix(l, " ") {
				t.Errorf("help %s: a line of %d columns, more than %d:\n%s", name, len(l), helpWidth, l)
			}
		}
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	const warning = `dynaflux/page_views.sql:3:1: warning DF2501: page_views: LOG RETENTION FOREVER keeps every event in DynamoDB; the table and the cost of projecting it keep growing
  hint: keep a short log (the default is 2 DAY) and use the archive as the permanent record; dynaflux explain shows the cost
`
	tests := []struct {
		name    string
		project string
		subdir  string // the working directory, within the project
		args    []string
		want    result
	}{
		{"no diagnostics", "pages", "", nil, result{0, "", ""}},
		{"warnings only", "warning", "", nil, result{0, "", warning}},
		{"from a subdirectory, with paths from the project root", "warning", "dynaflux", nil, result{0, "", warning}},
		{"errors", "errors", "", nil, result{1, "", errorsDiagnostics}},
		{
			"an argument", "pages", "", []string{"dynaflux"},
			result{2, "", "dynaflux validate: unexpected arguments: dynaflux\nRun 'dynaflux help validate' for usage.\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := filepath.Join(writeProject(t, tt.project), tt.subdir)
			if got := run(dir, append([]string{"validate"}, tt.args...)...); got != tt.want {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestValidateWithoutProject(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	want := result{2, "", "error DF6001: no dynaflux.yaml in the directory or any parent: " + dir +
		"\n  hint: run the command inside your project, or create dynaflux.yaml at its root\n"}
	if got := run(dir, "validate"); got != want {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
}

func TestValidateUnreadableFile(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root can read a file without read permission")
	}
	dir := writeProject(t, "warning")
	if err := os.Chmod(filepath.Join(dir, "dynaflux", "page_views.sql"), 0); err != nil {
		t.Fatal(err)
	}
	got := run(dir, "validate")
	if want := "dynaflux/page_views.sql:1:1: error DF6002: "; got.status != 2 || !strings.HasPrefix(got.stderr, want) {
		t.Errorf("got %+v, want status 2 and stderr starting with %q", got, want)
	}
}

func TestExplain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		project string
		args    []string
		// want is the result, except that its stdout is a prefix of the actual one.
		want result
	}{
		{
			"defaults", "pages", []string{"page_daily"},
			result{0, "view page_daily, from stream page_views\n" +
				"  assumptions  -rate 1000  -event-kb 1  -rows page_daily=1000  -uniq-new page_daily=0.3\n", ""},
		},
		{
			"flags before and after the name", "pages",
			[]string{"-rate", "2000", "-event-kb=2", "page_daily", "-rows", "page_daily=50", "-uniq-new", "page_daily=0.5"},
			result{0, "view page_daily, from stream page_views\n" +
				"  assumptions  -rate 2000  -event-kb 2  -rows page_daily=50  -uniq-new page_daily=0.5\n", ""},
		},
		{
			"horizon in days", "warning", []string{"page_views", "-horizon", "30d"},
			result{0, "stream page_views\n  assumptions  -rate 1000  -event-kb 1  -horizon 30d\n", ""},
		},
		{
			"horizon as a Go duration", "warning", []string{"page_views", "-horizon", "36h"},
			result{0, "stream page_views\n  assumptions  -rate 1000  -event-kb 1  -horizon 36h0m0s\n", ""},
		},
		{"project with errors", "errors", []string{"page_referrer"}, result{1, "", errorsDiagnostics}},
		{
			"unknown name", "pages", []string{"page_view"},
			result{2, "", "dynaflux explain: no stream, view or query named page_view; did you mean page_views?\n"},
		},
		{
			"no name", "pages", nil,
			result{2, "", "dynaflux explain: name one stream, view or query\nRun 'dynaflux help explain' for usage.\n"},
		},
		{
			"two names", "pages", []string{"page_views", "page_daily"},
			result{2, "", "dynaflux explain: name one stream, view or query\nRun 'dynaflux help explain' for usage.\n"},
		},
		{
			"workload out of range", "pages", []string{"page_views", "-rate", "0"},
			result{2, "", "dynaflux explain: -rate must be over 0 and at most 10,000,000 events per second, not 0\n"},
		},
		{
			"per-view flag without a view", "pages", []string{"page_views", "-rows", "50"},
			result{2, "", "dynaflux explain: invalid value \"50\" for flag -rows: want view=value\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"per-view flag that isn't a number", "pages", []string{"page_views", "-pass", "page_daily=most"},
			result{2, "", "dynaflux explain: invalid value \"page_daily=most\" for flag -pass: \"most\" isn't a number\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"per-view flag repeated for a view", "pages", []string{"page_views", "-rows", "page_daily=5", "-rows", "page_daily=6"},
			result{2, "", "dynaflux explain: invalid value \"page_daily=6\" for flag -rows: page_daily is already set\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"horizon in fractional days", "warning", []string{"page_views", "-horizon", "1.5d"},
			result{2, "", "dynaflux explain: invalid value \"1.5d\" for flag -horizon: want a whole number of days, such as 365d\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"horizon past the longest duration", "warning", []string{"page_views", "-horizon", "106752d"},
			result{2, "", "dynaflux explain: invalid value \"106752d\" for flag -horizon: want a whole number of days, such as 365d\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"horizon past the shortest duration", "warning", []string{"page_views", "-horizon=-106752d"},
			result{2, "", "dynaflux explain: invalid value \"-106752d\" for flag -horizon: want a whole number of days, such as 365d\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
		{
			"no days", "warning", []string{"page_views", "-horizon", "0d"},
			result{2, "", "dynaflux explain: invalid value \"0d\" for flag -horizon: want a whole number of days, such as 365d\n" +
				"Run 'dynaflux help explain' for usage.\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := run(writeProject(t, tt.project), append([]string{"explain"}, tt.args...)...)
			if got.status != tt.want.status || !strings.HasPrefix(got.stdout, tt.want.stdout) || got.stderr != tt.want.stderr {
				t.Errorf("got  %+v\nwant %+v, with stdout as a prefix", got, tt.want)
			}
		})
	}
}

func TestExplainReportWithError(t *testing.T) {
	t.Parallel()
	got := run(writeProject(t, "oversized"), "explain", "page_daily")
	want := "\nerror page_daily: rows reach 590 KB, over DynamoDB's 400 KB item limit: every write to them would fail\n" +
		"  hint: shorten LOG RETENTION on page_views (fewer shards), or runtime.prune_window_seconds; SPLIT 2 keeps each item under the limit\n"
	if got.status != 1 || !strings.HasSuffix(got.stdout, want) || got.stderr != "" {
		t.Errorf("got %+v, want status 1 and stdout ending with %q", got, want)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("disk full")
}

func TestDiagnosticsWriteFails(t *testing.T) {
	t.Parallel()
	if status := cli.Run(t.Context(), []string{"validate"}, nil, io.Discard, failingWriter{}, writeProject(t, "errors")); status != 2 {
		t.Errorf("got status %d, want 2", status)
	}
}

func TestOutputWriteFails(t *testing.T) {
	t.Parallel()
	var stderr strings.Builder
	status := cli.Run(t.Context(), []string{"version"}, nil, failingWriter{}, &stderr, t.TempDir())
	if want := "dynaflux: write output: disk full\n"; status != 2 || stderr.String() != want {
		t.Errorf("got status %d and stderr %q, want 2 and %q", status, stderr.String(), want)
	}
}
