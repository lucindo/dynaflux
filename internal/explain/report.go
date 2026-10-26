// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
)

const usPrefix = "us-"

// Report is what dynaflux explain prints for one stream, view or query.
type Report struct {
	Text string
	// Failed is set when the report holds an error: the design can't work as it is, such as
	// a view whose rows outgrow DynamoDB's item limit.
	Failed bool
}

// Explain reports on the stream, view or query called name, in a project that
// validate.Load compiled. Its error is for a name the project doesn't define, or a
// workload out of range.
func Explain(cfg *config.Config, cat *catalog.Catalog, w Workload, name string) (Report, error) {
	if err := w.check(cat); err != nil {
		return Report{}, err
	}
	e := &explainer{cfg: cfg, cat: cat, w: w}
	s, isStream := cat.Stream(name)
	v, isView := cat.View(name)
	qi := slices.IndexFunc(cat.Queries, func(q catalog.Query) bool { return q.Name == name })
	var err error
	switch {
	case isStream:
		err = e.streamReport(s)
	case isView:
		err = e.viewReport(v)
	case qi >= 0:
		err = e.queryReport(&cat.Queries[qi])
	default:
		return Report{}, unknown(cat, name)
	}
	if err != nil {
		return Report{}, err
	}
	failed := e.writeFindings()
	return Report{Text: e.b.String(), Failed: failed}, nil
}

func unknown(cat *catalog.Catalog, name string) error {
	var names []string
	for _, s := range cat.Streams {
		names = append(names, s.Name)
	}
	for _, v := range cat.Views {
		names = append(names, v.Name)
	}
	for _, q := range cat.Queries {
		names = append(names, q.Name)
	}
	msg := "no stream, view or query named " + name
	if s, ok := diag.Suggest(name, names); ok {
		msg += "; did you mean " + s + "?"
	}
	return errors.New(msg)
}

// explainer writes one report.
type explainer struct {
	cfg      *config.Config
	cat      *catalog.Catalog
	w        Workload
	b        strings.Builder
	findings []finding
}

// finding is a warning, or an error when the design can't work as it is.
type finding struct {
	err     bool
	subject string // the stream or view it's about
	message string
	hint    string
}

// estimate is a stream's plan and its cost.
type estimate struct {
	streamPlan

	cost StreamCost
}

func (e *explainer) estimate(s *catalog.Stream) (estimate, error) {
	p := newStreamPlan(e.cfg, e.cat, e.w, s)
	c, err := Estimate(p.store, p.model)
	if err != nil {
		return estimate{}, fmt.Errorf("stream %s: %w", s.Name, err)
	}
	return estimate{p, c}, nil
}

// header starts a report: its title, the workload it assumes and the price basis.
func (e *explainer) header(title string, flags []string) {
	e.b.WriteString(title + "\n")
	e.line("assumptions", strings.Join(flags, "  "))
	e.line("", "(traffic isn't in the SQL or dynaflux.yaml; set it with these flags)")
	prices := "us-east-1 list prices, ±30%: for comparing designs, not a quote"
	var other []string
	for _, r := range e.cfg.Regions {
		if !strings.HasPrefix(r, usPrefix) {
			other = append(other, r)
		}
	}
	if len(other) > 0 {
		prices += "; prices in " + strings.Join(other, ", ") + " are higher"
	}
	e.line("prices", prices)
}

// line writes a labeled line of a section.
func (e *explainer) line(label, text string) {
	fmt.Fprintf(&e.b, "  %-*s  %s\n", sectionPadding, label, text)
}

func (e *explainer) section(title string) {
	e.b.WriteString("\n" + title + "\n")
}

// flags returns the workload flags that est depends on, for views[i] alone when i >= 0.
func (est *estimate) flags(w Workload, i int) []string {
	flags := []string{"--rate " + flagValue(w.Rate), "--event-kb " + flagValue(w.EventKB)}
	seen := map[string]bool{}
	for j, vp := range est.views {
		if (i >= 0 && j != i) || seen[vp.view] {
			continue
		}
		seen[vp.view] = true
		m := est.model.Views[j]
		if m.Shared {
			flags = append(flags, "--rows "+vp.view+"="+flagValue(m.ActiveRows))
		}
		if m.UniqExacts > 0 {
			flags = append(flags, "--uniq-new "+vp.view+"="+flagValue(m.UniqNew))
		}
		if vp.version.Where != nil || vp.version.ArrayJoin != nil {
			flags = append(flags, "--pass "+vp.view+"="+flagValue(m.Pass))
		}
	}
	if est.model.Forever {
		flags = append(flags, "--horizon "+FormatHorizon(w.Horizon))
	}
	return flags
}

// FormatHorizon formats d as --horizon takes it: in whole days, such as 365d, or as a Go
// duration.
func FormatHorizon(d time.Duration) string {
	if d%day == 0 {
		return strconv.FormatInt(int64(d/day), 10) + "d"
	}
	return d.String()
}

func (e *explainer) writeFindings() bool {
	failed := false
	for i, f := range e.findings {
		if i == 0 {
			e.b.WriteString("\n")
		}
		severity := "warning"
		if f.err {
			severity, failed = "error", true
		}
		fmt.Fprintf(&e.b, "%s %s: %s\n", severity, f.subject, f.message)
		if f.hint != "" {
			e.b.WriteString("  hint: " + f.hint + "\n")
		}
	}
	return failed
}

func retentionText(r catalog.Retention) string {
	p, ok := r.Period()
	if !ok {
		return "FOREVER"
	}
	return p.String()
}

func capacityText(c Capacity) string {
	if !c.Provisioned {
		return "on-demand"
	}
	return fmt.Sprintf("provisioned, autoscaling to %s%% of reads and %s%% of writes",
		num(c.ReadTarget*percent), num(c.WriteTarget*percent))
}

// andList joins words as a sentence lists them: "a", "a and b", "a, b and c".
func andList(words []string) string {
	if len(words) <= 1 {
		return strings.Join(words, "")
	}
	return strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}

// plural picks the word for n as num prints it, so that 1.001 events read as "1 event".
func plural(n float64, one, many string) string {
	if num(n) == "1" {
		return one
	}
	return many
}
