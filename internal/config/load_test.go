// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config_test

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
)

// completeExample is the complete example in docs/spec/config.md.
const completeExample = `store: analytics
prefix: prod-                         # not default (default: no prefix)
regions: [us-east-1, us-west-2, ap-northeast-1]
sql: ["dynaflux/**/*.sql"]

go:
  package: flux
  out: internal/flux

runtime:
  out: cmd/dynaflux-runtime
  arch: arm64                         # arm64 | x86_64
  timeout_seconds: 60
  prune_window_seconds: 960
  memory_mb: 1024
  batching_window_seconds: 1
  split: store                        # store | stream

infra:
  format: sam
  out: infra/dynaflux

defaults:
  capacity: on_demand                 # on_demand | provisioned
  view_mode: async                    # async | sync
  log_retention: 2d                   # <n>h | <n>d | <n>w | forever

tables:                               # not default (default: no overrides)
  page_daily:                         # a view: applies to the table of every version
    capacity: provisioned
    autoscaling:
      read:  {min: 5, max: 500}       # target_percent defaults to 70
      write: {min: 5, max: 1000, target_percent: 60}

archive:
  enabled: true
  catalog: glue                       # glue | s3tables
  regions: all                        # all | a list of entries from regions
  events: all                         # all | local
  streams:                            # not default (default: no overrides)
    debug_events: {enabled: false}    # validate warns (DF6504)

http:
  frontend: http_api                  # not default (default: function_url)
  auth: iam                           # allowed values depend on frontend
  allow_public: false
  domain:                             # not default (default: none)
    name: flux.example.com
    routing: latency
`

// base sets the required keys on lines 1 and 2, so test cases start on line 3.
const base = "store: analytics\nregions: [us-east-1, us-west-2]\n"

func load(src string) (*config.Config, []diag.Diagnostic) {
	return config.Load(fstest.MapFS{config.FileName: {Data: []byte(src)}})
}

// render writes each diagnostic as "line:col severity code".
func render(ds []diag.Diagnostic) []string {
	var out []string
	for _, d := range ds {
		out = append(out, fmt.Sprintf("%d:%d %s %s", d.Pos.Line, d.Pos.Col, d.Severity, d.Code))
	}
	return out
}

func pos(line, col int) diag.Pos {
	return diag.Pos{File: config.FileName, Line: line, Col: col}
}

func TestLoadDiagnostics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{"DF6003 syntax error", "store: a: b\n", []string{"1:9 error DF6003"}},
		{"DF6004 empty file", "", []string{"1:1 error DF6004"}},
		{"DF6004 only comments", "# nothing yet\n", []string{"1:1 error DF6004"}},
		{"DF6004 top-level list", "- store: analytics\n", []string{"1:1 error DF6004"}},
		{"DF6004 two documents", "store: analytics\n---\nregions: [us-east-1]\n", []string{"2:1 error DF6004"}},
		{"DF6005 unknown top-level key", base + "regoins: [us-east-1]\n", []string{"3:1 error DF6005"}},
		{"DF6005 unknown nested key", base + "runtime:\n  memmory_mb: 64\n", []string{"4:3 error DF6005"}},
		{"DF6006 duplicate key hides other errors", base + "store: web\nbogus: 1\n", []string{"3:1 error DF6006"}},
		{"DF6006 nested duplicate", base + "runtime:\n  arch: arm64\n  arch: x86_64\n", []string{"5:3 error DF6006"}},
		{"DF6014 anchor and alias", base + "go: &g {out: a}\ninfra: *g\n", []string{"3:5 error DF6014", "4:8 error DF6014"}},
		{"DF6014 merge key", base + "go:\n  <<: {out: a}\n", []string{"4:3 error DF6014"}},
		{"DF6014 explicit tag", base + "prefix: !!str prod-\n", []string{"3:9 error DF6014"}},
		{"DF6014 non-specific tag", base + "prefix: ! prod-\n", []string{"3:9 error DF6014"}},
		{"DF6007 string for a list", "store: analytics\nregions: us-east-1\n", []string{"2:10 error DF6007"}},
		{"DF6007 yes for a boolean", base + "archive:\n  enabled: yes\n", []string{"4:12 error DF6007"}},
		{"DF6007 quoted boolean", base + "archive:\n  enabled: \"true\"\n", []string{"4:12 error DF6007"}},
		{
			"DF6007 integer spellings",
			base + "runtime:\n  memory_mb: 1024.0\n  timeout_seconds: 0x3c\n  batching_window_seconds: \"1\"\n",
			[]string{"4:14 error DF6007", "5:20 error DF6007", "6:28 error DF6007"},
		},
		{"DF6007 leading zero", base + "runtime:\n  memory_mb: 0128\n", []string{"4:14 error DF6007"}},
		{"DF6007 number for a string", "store: 123\nregions: [us-east-1]\n", []string{"1:8 error DF6007"}},
		{"DF6007 null list entry", "store: analytics\nregions: [us-east-1, null]\n", []string{"2:22 error DF6007"}},
		{"DF6007 scalar for a mapping", base + "runtime: fast\n", []string{"3:10 error DF6007"}},
		{"DF6007 number for a duration", base + "defaults:\n  log_retention: 48\n", []string{"4:18 error DF6007"}},
		{"DF6007 list as a key", base + "? [a, b]\n: c\n", []string{"3:3 error DF6007"}},
		{
			"DF6007 mapping as a tables key",
			base + "tables:\n  ? {a: b}\n  : {capacity: on_demand}\n",
			[]string{"4:5 error DF6007"},
		},
		{"DF6008 missing store", "regions: [us-east-1]\n", []string{"1:1 error DF6008"}},
		{"DF6008 null store", "store:\nregions: [us-east-1]\n", []string{"1:1 error DF6008"}},
		{"DF6008 null tables entry", base + "tables: {page_daily: }\n", []string{"3:10 error DF6008"}},
		{"DF6008 empty archive.streams entry", base + "archive:\n  streams:\n    page_views: {}\n", []string{"5:17 error DF6008"}},
		{"DF6008 null archive.streams entry", base + "archive: {streams: {page_views: }}\n", []string{"3:21 error DF6008"}},
		{
			"DF6008 missing domain name",
			base + "http:\n  frontend: http_api\n  domain: {routing: latency}\n",
			[]string{"5:11 error DF6008"},
		},
		{
			"DF6008 missing autoscaling min",
			base + "defaults:\n  capacity: provisioned\n  autoscaling:\n    read: {max: 5}\n    write: {min: 1, max: 5}\n",
			[]string{"6:11 error DF6008"},
		},
		{"DF6009 wrong case", base + "defaults:\n  capacity: ON_DEMAND\n", []string{"4:13 error DF6009"}},
		{
			"DF6009 hides the autoscaling rules",
			base + "defaults:\n  capacity: provisioned_\n  autoscaling: {read: {min: 1, max: 2}, write: {min: 1, max: 2}}\n",
			[]string{"4:13 error DF6009"},
		},
		{"DF6009 archive regions", base + "archive:\n  regions: everywhere\n", []string{"4:12 error DF6009"}},
		{
			"DF6009 split hides the store-wide projector name",
			"store: " + strings.Repeat("a", 55) + "\nregions: [us-east-1]\nruntime: {split: Stream}\n",
			[]string{"1:8 error DF6110", "3:18 error DF6009"}, // the bucket only
		},
		{"DF6010 below the Lambda minimum", base + "runtime:\n  memory_mb: 64\n", []string{"4:14 error DF6010"}},
		{"DF6010 prune window of 0", base + "runtime:\n  prune_window_seconds: 0\n", []string{"4:25 error DF6010"}},
		{"DF6010 prune window over 25 hours", base + "runtime:\n  prune_window_seconds: 90001\n", []string{"4:25 error DF6010"}},
		{
			"DF6010 beyond 64 bits",
			base + "runtime:\n  timeout_seconds: 99999999999999999999\n",
			[]string{"4:20 error DF6010"},
		},
		{
			"DF6010 autoscaling ranges",
			base + "defaults:\n  capacity: provisioned\n  autoscaling:\n    read: {min: 0, max: 5}\n" +
				"    write: {min: 1, max: 5, target_percent: 95}\n",
			[]string{"6:17 error DF6010", "7:45 error DF6010"},
		},
		{"DF6011 empty regions", "store: analytics\nregions: []\n", []string{"2:10 error DF6011"}},
		{"DF6011 empty sql", base + "sql: []\n", []string{"3:6 error DF6011"}},
		{"DF6011 empty archive.regions counts as all", base + "archive: {regions: [], events: local}\n", []string{"3:20 error DF6011"}},
		{"DF6012 repeated region", "store: analytics\nregions: [us-east-1, us-east-1]\n", []string{"2:22 error DF6012"}},
		{"DF6012 repeated archive region", base + "archive:\n  regions: [us-east-1, us-east-1]\n", []string{"4:24 error DF6012"}},
		{
			"DF6012 pattern repeated after cleaning",
			base + "sql: [dynaflux/*.sql, ./dynaflux/*.sql]\n",
			[]string{"3:23 error DF6012"},
		},
		{"DF6101 uppercase store", "store: Analytics\nregions: [us-east-1]\n", []string{"1:8 error DF6101"}},
		{"DF6101 store ends with -", "store: analytics-\nregions: [us-east-1]\n", []string{"1:8 error DF6101"}},
		{"DF6101 underscore in store", "store: web_analytics\nregions: [us-east-1]\n", []string{"1:8 error DF6101"}},
		{"DF6102 prefix without -", base + "prefix: prod\n", []string{"3:9 error DF6102"}},
		{"DF6103 bad region", "store: analytics\nregions: [us-east]\n", []string{"2:11 error DF6103"}},
		{"DF6103 one-word region", "store: analytics\nregions: [useast-1]\n", []string{"2:11 error DF6103"}},
		{"DF6103 FIPS pseudo-region", "store: analytics\nregions: [fips-us-east-1]\n", []string{"2:11 error DF6103"}},
		{"DF6103 FIPS in the middle", "store: analytics\nregions: [us-fips-east-1]\n", []string{"2:11 error DF6103"}},
		{"DF6104 no account regional namespace", "store: analytics\nregions: [us-east-1, me-south-1]\n", []string{"2:22 error DF6104"}},
		{
			"DF6110 projector and bucket names",
			"store: " + strings.Repeat("a", 55) + "\nregions: [us-east-1]\n",
			[]string{"1:8 error DF6110", "1:8 error DF6110"},
		},
		{
			"DF6110 bucket name counts the prefix",
			"store: " + strings.Repeat("a", 33) + "\nprefix: prod-\nregions: [us-east-1]\n",
			[]string{"1:8 error DF6110"},
		},
		{
			"DF6110 bucket name in each region",
			"store: " + strings.Repeat("a", 33) + "\nregions: [us-east-1, ap-northeast-1]\n",
			[]string{"1:8 error DF6110"},
		},
		{
			"DF6111 S3 Tables namespace starts with aws",
			base + "prefix: aws-\narchive:\n  catalog: s3tables\n",
			[]string{"1:8 error DF6111", "5:12 error DF6506"},
		},
		{"DF6112 store starts with xn--", "store: xn--analytics\nregions: [us-east-1]\n", []string{"1:8 error DF6112"}},
		{"DF6112 counts the prefix", "store: s3-demo-x\nprefix: amzn-\nregions: [us-east-1]\n", []string{"1:8 error DF6112"}},
		{"DF6201 absolute pattern", base + "sql: [/abs/*.sql]\n", []string{"3:7 error DF6201"}},
		{"DF6201 .. before cleaning", base + "sql: [dynaflux/**/../*.sql]\n", []string{"3:7 error DF6201"}},
		{"DF6202 empty pattern", base + "sql: [\"\"]\n", []string{"3:7 error DF6202"}},
		{"DF6202 trailing slash", base + "sql: [dynaflux/]\n", []string{"3:7 error DF6202"}},
		{"DF6202 unclosed class", base + "sql: [\"dynaflux/[a.sql\"]\n", []string{"3:7 error DF6202"}},
		{"DF6203 ** inside a segment", base + "sql: [\"dynaflux/**.sql\"]\n", []string{"3:7 error DF6203"}},
		{"DF6301 uppercase package", base + "go:\n  package: Flux\n", []string{"4:12 error DF6301"}},
		{"DF6301 Go keyword", base + "go:\n  package: func\n", []string{"4:12 error DF6301"}},
		{"DF6301 runtime", base + "go:\n  package: runtime\n", []string{"4:12 error DF6301"}},
		{"DF6301 log", base + "go:\n  package: log\n", []string{"4:12 error DF6301"}},
		{"DF6302 .. before cleaning", base + "go:\n  out: internal/../flux\n", []string{"4:8 error DF6302"}},
		{"DF6302 absolute", base + "infra:\n  out: /abs\n", []string{"4:8 error DF6302"}},
		{"DF6302 project root", base + "infra:\n  out: ./\n", []string{"4:8 error DF6302"}},
		{"DF6302 backslash", base + "infra:\n  out: 'infra\\out'\n", []string{"4:8 error DF6302"}},
		{"DF6302 empty", base + "infra:\n  out: \"\"\n", []string{"4:8 error DF6302"}},
		{"DF6303 same as a default", base + "infra:\n  out: internal/flux\n", []string{"4:8 error DF6303"}},
		{"DF6303 points at the later one", base + "infra:\n  out: ./gen\ngo:\n  out: gen\n", []string{"6:8 error DF6303"}},
		{
			"DF6303 beside an unrelated error",
			base + "runtime:\n  memory_mb: 64\ninfra:\n  out: internal/flux\n",
			[]string{"4:14 error DF6010", "6:8 error DF6303"},
		},
		{
			"DF6303 beside an invalid output",
			base + "infra:\n  out: /abs\ngo:\n  out: gen\nruntime:\n  out: gen\n",
			[]string{"4:8 error DF6302", "8:8 error DF6303"},
		},
		{"DF6304 default runtime.out", base + "go:\n  out: pkg/internal/flux\n", []string{"4:8 error DF6304"}},
		{
			"DF6304 beside an unrelated error",
			base + "go:\n  package: Flux\n  out: pkg/internal/flux\n",
			[]string{"4:12 error DF6301", "5:8 error DF6304"},
		},
		{
			"DF6304 runtime.out set",
			base + "go:\n  out: pkg/internal/flux\nruntime:\n  out: cmd/rt\n",
			[]string{"6:8 error DF6304"},
		},
		{
			"DF6304 sibling that shares a name prefix",
			base + "go: {out: pkg/internal/flux}\nruntime: {out: pkg2/rt}\n",
			[]string{"4:16 error DF6304"},
		},
		{
			"DF6305 prune window under timeout + 60",
			base + "runtime:\n  timeout_seconds: 300\n  prune_window_seconds: 359\n",
			[]string{"5:25 error DF6305"},
		},
		{"DF6305 against the default timeout", base + "runtime:\n  prune_window_seconds: 119\n", []string{"4:25 error DF6305"}},
		{
			"DF6305 needs a valid timeout",
			base + "runtime:\n  timeout_seconds: 901\n  prune_window_seconds: 100\n",
			[]string{"4:20 error DF6010"},
		},
		{"DF6306 at timeout + 60", base + "runtime:\n  prune_window_seconds: 120\n", []string{"4:25 warning DF6306"}},
		{"DF6306 just under 960", base + "runtime:\n  prune_window_seconds: 959\n", []string{"4:25 warning DF6306"}},
		{
			"DF6305 just under 960 with the longest timeout",
			base + "runtime:\n  timeout_seconds: 900\n  prune_window_seconds: 959\n",
			[]string{"5:25 error DF6305"},
		},
		{"DF6401 uppercase unit", base + "defaults:\n  log_retention: 2D\n", []string{"4:18 error DF6401"}},
		{"DF6401 two pairs", base + "defaults:\n  log_retention: 1d12h\n", []string{"4:18 error DF6401"}},
		{"DF6401 leading zero", base + "defaults:\n  log_retention: 02d\n", []string{"4:18 error DF6401"}},
		{"DF6402 under a day", base + "defaults:\n  log_retention: 23h\n", []string{"4:18 error DF6402"}},
		{"DF6402 over 36525 days", base + "defaults:\n  log_retention: 36526d\n", []string{"4:18 error DF6402"}},
		{"DF6402 over 5217 weeks", base + "defaults:\n  log_retention: 5218w\n", []string{"4:18 error DF6402"}},
		{"DF6404 defaults", base + "defaults:\n  capacity: provisioned\n", []string{"4:13 error DF6404"}},
		{"DF6404 table", base + "tables:\n  page_daily: {capacity: provisioned}\n", []string{"4:26 error DF6404"}},
		{
			"DF6405 autoscaling with on_demand",
			base + "defaults:\n  autoscaling: {read: {min: 1, max: 2}, write: {min: 1, max: 2}}\n",
			[]string{"4:16 error DF6405"},
		},
		{
			"DF6406 min over max",
			base + "defaults:\n  capacity: provisioned\n  autoscaling:\n    read: {min: 10, max: 5}\n    write: {min: 1, max: 5}\n",
			[]string{"6:17 error DF6406"},
		},
		{"DF6501 archive region not in regions", base + "archive:\n  regions: [eu-west-1]\n", []string{"4:13 error DF6501"}},
		{
			"DF6502 local events without all regions",
			base + "archive:\n  regions: [us-east-1]\n  events: local\n",
			[]string{"5:11 error DF6502"},
		},
		{"DF6506 S3 Tables", base + "archive:\n  catalog: s3tables\n", []string{"4:12 error DF6506"}},
		{"DF6601 auth unsupported", base + "http:\n  frontend: http_api\n  auth: none\n", []string{"5:9 error DF6601"}},
		{"DF6602 none without allow_public", base + "http:\n  auth: none\n", []string{"4:9 error DF6602"}},
		{"DF6603 public", base + "http:\n  auth: none\n  allow_public: true\n", []string{"4:9 warning DF6603"}},
		{"DF6604 allow_public with iam", base + "http:\n  allow_public: true\n", []string{"4:17 warning DF6604"}},
		{"DF6605 domain on a function URL", base + "http:\n  domain: {name: flux.example.com}\n", []string{"4:11 error DF6605"}},
		{
			"DF6606 uppercase domain",
			base + "http:\n  frontend: http_api\n  domain: {name: Flux.example.com}\n",
			[]string{"5:18 error DF6606"},
		},
		{
			"DF6606 single label",
			base + "http:\n  frontend: http_api\n  domain: {name: localhost}\n",
			[]string{"5:18 error DF6606"},
		},
		{
			"DF6606 label of 64 characters",
			base + "http:\n  frontend: http_api\n  domain: {name: " + strings.Repeat("a", 64) + ".com}\n",
			[]string{"5:18 error DF6606"},
		},
		{
			"DF6606 name of 254 characters",
			base + "http:\n  frontend: http_api\n  domain: {name: " + strings.Repeat("a.", 126) + "ab}\n",
			[]string{"5:18 error DF6606"},
		},
		{"DF6607 api_key", base + "http:\n  frontend: rest_api\n  auth: api_key\n", []string{"5:9 warning DF6607"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, ds := load(tt.src)
			if got := render(ds); !slices.Equal(got, tt.want) {
				t.Errorf("diagnostics = %q, want %q\n%v", got, tt.want, ds)
			}
			if (cfg != nil) == diag.HasErrors(ds) {
				t.Errorf("config is nil: %v, with errors: %v", cfg == nil, diag.HasErrors(ds))
			}
		})
	}
}

func TestLoadAccepts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		src  string
	}{
		{"complete example", completeExample},
		{"bucket name of 63 characters", "store: " + strings.Repeat("a", 32) + "\nprefix: prod-\nregions: [us-east-1]\n"},
		{"bucket name of 63 characters in ap-northeast-1", "store: " + strings.Repeat("a", 32) + "\nregions: [ap-northeast-1, us-east-1]\n"},
		{"aws prefix with glue", base + "prefix: aws-\n"},
		{"empty prefix", base + "prefix: \"\"\n"},
		{"null sections", base + "go:\nruntime: ~\ntables:\narchive: null\n"},
		{"one day in hours", base + "defaults: {log_retention: 24h}\n"},
		{"36525 days", base + "defaults: {log_retention: 36525d}\n"},
		{"5217 weeks", base + "defaults: {log_retention: 5217w}\n"},
		{"876600 hours", base + "defaults: {log_retention: 876600h}\n"},
		{"runtime.out inside the internal parent", base + "go:\n  out: pkg/internal/flux\nruntime:\n  out: pkg/cmd/rt\n"},
		{"go.out under a top-level internal", base + "go: {out: internal/gen/flux}\nruntime: {out: tools/rt}\n"},
		{"pattern cleaned", base + "sql: [./dynaflux/**/*.sql, \"dynaflux/_drafts/*.sql\"]\n"},
		{"quoted <<", base + "tables: {'<<': {capacity: on_demand}}\n"},
		{"auth none confirmed is only a warning", base + "http: {auth: none, allow_public: true}\n"},
		{"prune window of 960 with the longest timeout", base + "runtime: {timeout_seconds: 900, prune_window_seconds: 960}\n"},
		{"prune window of 25 hours", base + "runtime: {prune_window_seconds: 90000}\n"},
		{
			"domain name of 253 characters with a label of 63",
			base + "http: {frontend: http_api, domain: {name: " +
				strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 61) + "}}\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, ds := load(tt.src)
			if cfg == nil || diag.HasErrors(ds) {
				t.Errorf("Load reported errors: %v", ds)
			}
		})
	}
}

func TestLoadUnknownKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		src     string
		message string
		hint    string
	}{
		{"close key", base + "regoins: [us-east-1]\n", `unknown key "regoins"`, `did you mean "regions"?`},
		{
			"letter case",
			"STORE: analytics\nstore: analytics\nregions: [us-east-1]\n",
			`unknown key "STORE"`, `did you mean "store"?`,
		},
		{"nothing close", base + "go: {pkg: flux}\n", `unknown key "go.pkg"`, "the keys allowed here are package, out"},
		{
			"empty key",
			base + "'': 1\n",
			`unknown key ""`,
			"the keys allowed here are store, prefix, regions, sql, go, runtime, infra, defaults, tables, archive, http",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ds := load(tt.src)
			if len(ds) != 1 || ds[0].Code != "DF6005" || ds[0].Message != tt.message || ds[0].Hint != tt.hint {
				t.Errorf("diagnostics = %v, want one DF6005: %s, hint %q", ds, tt.message, tt.hint)
			}
		})
	}
}

// Following a hint must lead to a valid value, not to another error.
func TestLoadHint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		src  string
		code string
		hint string
	}{
		{
			"duration given as a number",
			base + "defaults:\n  log_retention: 48\n",
			"DF6007", "write a whole number and one unit, such as 36h, 2d or 1w, or write forever",
		},
		{"store given as a date", "store: 2001-12-14\nregions: [us-east-1]\n", "DF6007", "for example store: analytics"},
		{"prefix given as a number", base + "prefix: 7\n", "DF6007", "for example prefix: prod-"},
		{"package given as a boolean", base + "go:\n  package: true\n", "DF6007", "for example package: flux"},
		{"allowed value given as a number", base + "http:\n  frontend: 5\n", "DF6007", "use function_url, http_api or rest_api"},
		{"domain given as a list", base + "http:\n  frontend: http_api\n  domain: {name: [a]}\n", "DF6007", "for example name: flux.example.com"},
		{"path given as a number", base + "go:\n  out: 2024\n", "DF6007", `quote the value if YAML reads it as another type, as in "123"`},
		{"null list entry", "store: analytics\nregions: [us-east-1, null]\n", "DF6007", "for example regions: [us-east-1]"},
		{
			"empty nested list",
			base + "archive:\n  regions: []\n",
			"DF6011", "add at least one entry, such as regions: [us-east-1], or remove archive.regions to use its default",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ds := load(tt.src)
			if len(ds) != 1 || ds[0].Code != tt.code || ds[0].Hint != tt.hint {
				t.Errorf("diagnostics = %v, want one %s with hint %q", ds, tt.code, tt.hint)
			}
		})
	}
}

// The budgets are those of the Resource names table in docs/spec/config.md: P + region <= 46
// for the bucket, P <= 54 for the projector function, and P <= 55 for the backfill function.
// The bucket's is the tightest, so a function name is too long only together with it.
func TestLoadNameBudget(t *testing.T) {
	t.Parallel()
	bucket := func(base, region string, length, left int) string {
		return fmt.Sprintf("dynaflux.yaml:1:8: error DF6110: the S3 bucket name %s-123456789012-%s-an has %d characters; the limit is 63\n"+
			"  hint: shorten store or prefix: the S3 bucket name leaves them %d characters together", base, region, length, left)
	}
	tests := []struct {
		name string
		src  string
		want []string
	}{
		{
			"bucket",
			"store: " + strings.Repeat("a", 33) + "\nregions: [us-east-1, ap-northeast-1]\n",
			[]string{bucket(strings.Repeat("a", 33), "ap-northeast-1", 64, 32)},
		},
		{
			"projector",
			"store: " + strings.Repeat("a", 55) + "\nregions: [us-east-1]\n",
			[]string{
				bucket(strings.Repeat("a", 55), "us-east-1", 81, 37),
				"dynaflux.yaml:1:8: error DF6110: the projector function name " + strings.Repeat("a", 55) +
					"-projector has 65 characters; the limit is 64\n" +
					"  hint: shorten store or prefix: the projector function name leaves them 54 characters together",
			},
		},
		{
			"backfill",
			"store: " + strings.Repeat("a", 53) + "\nprefix: pr-\nregions: [us-east-1]\nruntime: {split: stream}\n",
			[]string{
				bucket("pr-"+strings.Repeat("a", 53), "us-east-1", 82, 37),
				"dynaflux.yaml:1:8: error DF6110: the backfill function name pr-" + strings.Repeat("a", 53) +
					"-backfill has 65 characters; the limit is 64\n" +
					"  hint: shorten store or prefix: the backfill function name leaves them 55 characters together",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ds := load(tt.src)
			got := make([]string, len(ds))
			for i, d := range ds {
				got[i] = d.String()
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("diagnostics:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

// Every pair of the front door table in docs/spec/config.md is accepted, and every other pair
// is DF6601.
func TestLoadAuth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		frontend string
		auth     string
		want     []string
	}{
		{"function_url", "iam", nil},
		{"function_url", "none", []string{"5:9 warning DF6603"}},
		{"function_url", "jwt", []string{"5:9 error DF6601"}},
		{"function_url", "lambda", []string{"5:9 error DF6601"}},
		{"function_url", "api_key", []string{"5:9 error DF6601"}},
		{"function_url", "cognito", []string{"5:9 error DF6601"}},
		{"http_api", "iam", nil},
		{"http_api", "jwt", nil},
		{"http_api", "lambda", nil},
		{"http_api", "none", []string{"5:9 error DF6601"}},
		{"http_api", "api_key", []string{"5:9 error DF6601"}},
		{"http_api", "cognito", []string{"5:9 error DF6601"}},
		{"rest_api", "iam", nil},
		{"rest_api", "api_key", []string{"5:9 warning DF6607"}},
		{"rest_api", "lambda", nil},
		{"rest_api", "cognito", nil},
		{"rest_api", "none", []string{"5:9 error DF6601"}},
		{"rest_api", "jwt", []string{"5:9 error DF6601"}},
	}
	for _, tt := range tests {
		t.Run(tt.frontend+" "+tt.auth, func(t *testing.T) {
			t.Parallel()
			src := base + "http:\n  frontend: " + tt.frontend + "\n  auth: " + tt.auth + "\n"
			if tt.auth == "none" {
				src += "  allow_public: true\n"
			}
			if _, ds := load(src); !slices.Equal(render(ds), tt.want) {
				t.Errorf("diagnostics = %q, want %q\n%v", render(ds), tt.want, ds)
			}
		})
	}
}

// The message and hint of DF6010 are the example in docs/spec/config.md.
func TestLoadMessage(t *testing.T) {
	t.Parallel()
	_, ds := load(strings.Replace(completeExample, "memory_mb: 1024", "memory_mb: 64", 1))
	want := "dynaflux.yaml:15:14: error DF6010: runtime.memory_mb is 64; it must be between 128 and 10240\n" +
		"  hint: Lambda functions take 128 to 10240 MB of memory"
	if len(ds) != 1 || ds[0].String() != want {
		t.Errorf("diagnostics = %v, want\n%s", ds, want)
	}
}

func TestLoadUnreadable(t *testing.T) {
	t.Parallel()
	cfg, ds := config.Load(fstest.MapFS{})
	if got, want := render(ds), []string{"1:1 error DF6002"}; cfg != nil || !slices.Equal(got, want) {
		t.Errorf("Load = %v, %q; want nil, %q", cfg, got, want)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, ds := load("store: analytics\nregions: [us-east-1, us-west-2]\n")
	if len(ds) != 0 {
		t.Fatalf("diagnostics: %v", ds)
	}
	want := &config.Config{
		Store:      "analytics",
		Regions:    []string{"us-east-1", "us-west-2"},
		RegionsPos: pos(2, 10),
		SQL:        []config.Pattern{{Glob: "dynaflux/**/*.sql", Pos: pos(1, 1)}},
		Go:         config.Go{Package: "flux", Out: "internal/flux"},
		Runtime: config.Runtime{
			Out: "cmd/dynaflux-runtime", Arch: config.ArchARM64, TimeoutSeconds: 60, PruneWindowSeconds: 960,
			MemoryMB: 1024, BatchingWindowSeconds: 1, Split: config.SplitStore,
		},
		Infra: config.Infra{Format: config.InfraFormatSAM, Out: "infra/dynaflux"},
		Defaults: config.Defaults{
			Capacity:     config.Capacity{Mode: config.CapacityOnDemand},
			ViewMode:     catalog.ModeAsync,
			LogRetention: catalog.RetainFor(catalog.Interval{N: 2, Unit: catalog.UnitDay}),
		},
		Archive: config.Archive{
			Enabled: true, Catalog: config.ArchiveCatalogGlue, Regions: []string{"us-east-1", "us-west-2"}, AllRegions: true,
			RegionPos: map[string]diag.Pos{"us-east-1": pos(2, 11), "us-west-2": pos(2, 22)},
			Events:    catalog.ArchiveEventsAll,
		},
		HTTP: config.HTTP{Frontend: config.FrontendFunctionURL, Auth: config.AuthIAM},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load =\n%+v\nwant\n%+v", cfg, want)
	}
}

// Positions are those of the complete example in docs/spec/config.md.
func TestLoadComplete(t *testing.T) {
	t.Parallel()
	cfg, ds := load(completeExample)
	if len(ds) != 0 {
		t.Fatalf("diagnostics: %v", ds)
	}
	want := &config.Config{
		Store:      "analytics",
		Prefix:     "prod-",
		Regions:    []string{"us-east-1", "us-west-2", "ap-northeast-1"},
		RegionsPos: pos(3, 10),
		SQL:        []config.Pattern{{Glob: "dynaflux/**/*.sql", Pos: pos(4, 7)}},
		Go:         config.Go{Package: "flux", Out: "internal/flux"},
		Runtime: config.Runtime{
			Out: "cmd/dynaflux-runtime", Arch: config.ArchARM64, TimeoutSeconds: 60, PruneWindowSeconds: 960,
			MemoryMB: 1024, BatchingWindowSeconds: 1, Split: config.SplitStore,
		},
		Infra: config.Infra{Format: config.InfraFormatSAM, Out: "infra/dynaflux"},
		Defaults: config.Defaults{
			Capacity:        config.Capacity{Mode: config.CapacityOnDemand},
			ViewMode:        catalog.ModeAsync,
			LogRetention:    catalog.RetainFor(catalog.Interval{N: 2, Unit: catalog.UnitDay}),
			LogRetentionPos: pos(26, 18),
		},
		Tables: []config.Table{{
			Name: "page_daily",
			Pos:  pos(29, 3),
			Capacity: config.Capacity{Mode: config.CapacityProvisioned, Autoscaling: &config.Autoscaling{
				Read:  config.Scaling{Min: 5, Max: 500, TargetPercent: 70},
				Write: config.Scaling{Min: 5, Max: 1000, TargetPercent: 60},
			}},
		}},
		Archive: config.Archive{
			Enabled: true, Catalog: config.ArchiveCatalogGlue,
			Regions:    []string{"us-east-1", "us-west-2", "ap-northeast-1"},
			AllRegions: true,
			RegionPos:  map[string]diag.Pos{"us-east-1": pos(3, 11), "us-west-2": pos(3, 22), "ap-northeast-1": pos(3, 33)},
			Events:     catalog.ArchiveEventsAll,
			Streams:    []config.ArchiveStream{{Name: "debug_events", Pos: pos(41, 5), Enabled: false, EnabledPos: pos(41, 29)}},
		},
		HTTP: config.HTTP{
			Frontend: config.FrontendHTTPAPI, Auth: config.AuthIAM,
			Domain: &config.Domain{Name: "flux.example.com", Routing: config.RoutingLatency},
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load =\n%+v\nwant\n%+v", cfg, want)
	}
}

func TestLoadSettings(t *testing.T) {
	t.Parallel()
	cfg, ds := load(base + `prefix: staging-
sql: [queries/*.sql]
go: {package: gen, out: pkg/gen}
runtime: {out: cmd/rt, arch: x86_64, timeout_seconds: 900, prune_window_seconds: 1000, memory_mb: 10240, batching_window_seconds: 0, split: stream}
infra: {out: deploy}
defaults:
  capacity: provisioned
  autoscaling: {read: {min: 1, max: 1}, write: {min: 2, max: 3, target_percent: 20}}
  view_mode: sync
  log_retention: forever
tables: {page_views: {capacity: on_demand}}
archive: {enabled: false, catalog: glue, regions: [us-west-2], events: all, streams: {page_views: {enabled: true}}}
http: {frontend: rest_api, auth: cognito}
`)
	if len(ds) != 0 {
		t.Fatalf("diagnostics: %v", ds)
	}
	want := &config.Config{
		Store:      "analytics",
		Prefix:     "staging-",
		Regions:    []string{"us-east-1", "us-west-2"},
		RegionsPos: pos(2, 10),
		SQL:        []config.Pattern{{Glob: "queries/*.sql", Pos: pos(4, 7)}},
		Go:         config.Go{Package: "gen", Out: "pkg/gen"},
		Runtime: config.Runtime{
			Out: "cmd/rt", Arch: config.ArchX8664, TimeoutSeconds: 900, PruneWindowSeconds: 1000, MemoryMB: 10240,
			BatchingWindowSeconds: 0, Split: config.SplitStream,
		},
		Infra: config.Infra{Format: config.InfraFormatSAM, Out: "deploy"},
		Defaults: config.Defaults{
			Capacity: config.Capacity{Mode: config.CapacityProvisioned, Autoscaling: &config.Autoscaling{
				Read:  config.Scaling{Min: 1, Max: 1, TargetPercent: 70},
				Write: config.Scaling{Min: 2, Max: 3, TargetPercent: 20},
			}},
			ViewMode:        catalog.ModeSync,
			LogRetention:    catalog.RetainForever(),
			LogRetentionPos: pos(12, 18),
		},
		Tables: []config.Table{{Name: "page_views", Pos: pos(13, 10), Capacity: config.Capacity{Mode: config.CapacityOnDemand}}},
		Archive: config.Archive{
			Enabled: false, Catalog: config.ArchiveCatalogGlue, Regions: []string{"us-west-2"},
			RegionPos: map[string]diag.Pos{"us-west-2": pos(14, 52)},
			Events:    catalog.ArchiveEventsAll,
			Streams:   []config.ArchiveStream{{Name: "page_views", Pos: pos(14, 87), Enabled: true, EnabledPos: pos(14, 109)}},
		},
		HTTP: config.HTTP{Frontend: config.FrontendRESTAPI, Auth: config.AuthCognito},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load =\n%+v\nwant\n%+v", cfg, want)
	}
}
