// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/explain"
)

const day = 24 * time.Hour

func explainCommand() command {
	return command{
		name:    "explain",
		args:    "[flags] <stream|view|query>",
		summary: "show the layout and cost of a stream, view or query",
		doc: `Explain shows the table layout, the cost per event and per query, and hot-key
warnings for the stream, view or query with the given name. Traffic isn't in the
SQL or dynaflux.yaml, so the flags set the workload the report assumes, and the
report starts with the values it used. -rows, -uniq-new and -pass take one view
each and can be repeated. Flags may also follow the name.

Exit status: 0 for a report, 1 when the project or the report has an error, and 2
for usage and I/O problems, including a name the project doesn't define.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			w := explain.DefaultWorkload()
			fs.Float64Var(&w.Rate, "rate", w.Rate, "average `events` per second appended to the stream, all regions together")
			fs.Float64Var(&w.EventKB, "event-kb", w.EventKB, "average event size, in `KB`")
			fs.Var((*perView)(&w.Rows), "rows", fmt.Sprintf(
				"how many rows a shared view writes to at a time, as `view=N` (default %v)", explain.DefaultActiveRows))
			fs.Var((*perView)(&w.UniqNew), "uniq-new", fmt.Sprintf(
				"the fraction of a view's source rows that bring its uniqExact aggregate a new value, as `view=F` (default %v)",
				explain.DefaultUniqNew))
			fs.Var((*perView)(&w.Pass), "pass", fmt.Sprintf(
				"a view's source rows per event, as `view=F`: the fraction of events WHERE keeps times the ARRAY JOIN fan-out (default %v)",
				explain.DefaultPass))
			fs.Var((*horizon)(&w.Horizon), "horizon",
				"the `age` at which to evaluate a stream kept FOREVER, in days such as 365d or a Go duration such as 36h")
			return func(c *cli, names []string) int { return c.explain(w, names) }
		},
	}
}

func (c *cli) explain(w explain.Workload, names []string) int {
	if len(names) != 1 {
		return c.usageError("explain", "name one stream, view or query")
	}
	cfg, cat, ds, status := c.load()
	if status != exitOK {
		return c.diagnostics(ds, status)
	}
	r, err := explain.Explain(cfg, cat, w, names[0])
	if err != nil {
		c.errorf("dynaflux explain: %v", err)
		return exitUsage
	}
	c.out.WriteString(r.Text)
	if r.Failed {
		return exitErrors
	}
	return exitOK
}

// perView is a repeatable flag of view=value pairs.
type perView map[string]float64

func (p *perView) String() string {
	return fmt.Sprint(map[string]float64(*p))
}

func (p *perView) Set(s string) error {
	name, value, ok := strings.Cut(s, "=")
	if !ok || name == "" {
		return errors.New("want view=value")
	}
	x, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("%q isn't a number", value)
	}
	if _, set := (*p)[name]; set {
		return fmt.Errorf("%s is already set", name)
	}
	if *p == nil {
		*p = perView{}
	}
	(*p)[name] = x
	return nil
}

// horizon is a duration flag that also takes whole days, as explain prints them.
type horizon time.Duration

func (h *horizon) String() string {
	return explain.FormatHorizon(time.Duration(*h))
}

func (h *horizon) Set(s string) error {
	if n, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.ParseInt(n, 10, 64)
		if err != nil || days < 1 || days > math.MaxInt64/int64(day) {
			return errors.New("want a whole number of days, such as 365d")
		}
		*h = horizon(time.Duration(days) * day)
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return errors.New("want days, such as 365d, or a Go duration, such as 36h")
	}
	*h = horizon(d)
	return nil
}
