// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/explain"
)

const queryProject = `-- dynaflux.yaml --
store: analytics
regions: [us-east-1]
-- dynaflux/s.sql --
CREATE STREAM page_views (user_id String, page String)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW page_hours
ORDER BY (page, day, hour)
PRIMARY KEY (page)
AS SELECT page, toStartOfDay(event_time) AS day, toStartOfHour(event_time) AS hour, count() AS n
FROM page_views
GROUP BY page, day, hour;
`

// reportLine returns the text of the first line of text labeled label.
func reportLine(t *testing.T, text, label string) string {
	t.Helper()
	for line := range strings.Lines(text) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), label+" "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatalf("no %q line in the report:\n%s", label, text)
	return ""
}

// The read stops after LIMIT rows only when it reads rows in the order the query returns
// them: the key order or its reverse, where ties are broken by the key columns in the
// direction of the last ORDER BY item.
func TestExplainLimit(t *testing.T) {
	t.Parallel()
	const (
		bounds = "LIMIT 3 stops each Query after that many rows"
		after  = "LIMIT 3 applies after the read, so it doesn't bound the items read"
	)
	tests := []struct {
		name, query, want string
	}{
		{"KeyOrder", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY day, hour LIMIT 3", bounds},
		{"NoOrderBy", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} LIMIT 3", bounds},
		{"Reverse", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY day DESC, hour DESC LIMIT 3", bounds},
		{"BoundColumnFirst", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY page DESC, day, hour LIMIT 3", bounds},
		{"AscThenDesc", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY day, hour DESC LIMIT 3", after},
		{"DescThenAsc", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY day DESC, hour LIMIT 3", after},
		{"DescTiesDesc", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY day DESC LIMIT 3", bounds},
		{"NonKeyColumn", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} ORDER BY n LIMIT 3", after},
		{"Expression", "SELECT day, hour, n + 1 AS m FROM page_hours WHERE page = {p:String} ORDER BY m LIMIT 3", after},
		{"Filter", "SELECT day, hour, n FROM page_hours WHERE page = {p:String} AND n > 5 ORDER BY day, hour LIMIT 3", after},
		{"Aggregation", "SELECT day, sum(n) AS total FROM page_hours WHERE page = {p:String} GROUP BY day ORDER BY day LIMIT 3", after},
		{"StreamTimeOrder", "SELECT event_time, page FROM page_views WHERE user_id = {u:String} ORDER BY event_time LIMIT 3", bounds},
		{
			"StreamReverse",
			"SELECT event_time, page FROM page_views WHERE user_id = {u:String} ORDER BY event_time DESC, event_id DESC LIMIT 3", bounds,
		},
		{"StreamLatestFirst", "SELECT event_time, page FROM page_views WHERE user_id = {u:String} ORDER BY event_time DESC LIMIT 3", bounds},
	}
	var project strings.Builder
	project.WriteString(queryProject)
	for _, tt := range tests {
		project.WriteString("\n-- name: " + tt.name + " :many\n" + tt.query + ";\n")
	}
	cfg, cat := load(t, []byte(project.String()))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := explain.Explain(cfg, cat, explain.DefaultWorkload(), tt.name)
			if err != nil {
				t.Fatal(err)
			}
			if got := reportLine(t, r.Text, "limit"); got != tt.want {
				t.Errorf("limit line %q, want %q", got, tt.want)
			}
		})
	}
}

// A string literal prints as SQL that reads back as the same value, with no raw control
// characters that a terminal would act on.
func TestExplainStringLiteral(t *testing.T) {
	t.Parallel()
	cfg, cat := load(t, []byte(queryProject+`
-- name: Odd :many
SELECT day, hour, n FROM page_hours WHERE page = 'it''s \\ café \x1b[31m\n\t\xe2\x80\xae';
`))
	r, err := explain.Explain(cfg, cat, explain.DefaultWorkload(), "Odd")
	if err != nil {
		t.Fatal(err)
	}
	want := `Query of page = 'it''s \\ café \x1b[31m\x0a\x09\xe2\x80\xae'`
	if got := reportLine(t, r.Text, "read"); got != want {
		t.Errorf("read line %q, want %q", got, want)
	}
	if i := strings.IndexFunc(r.Text, func(c rune) bool { return c != '\n' && !strconv.IsGraphic(c) }); i >= 0 {
		t.Errorf("the report holds %q at byte %d", r.Text[i], i)
	}
}
