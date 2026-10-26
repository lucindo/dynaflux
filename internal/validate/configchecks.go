// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
)

// checkConfigNames runs the checks that need both dynaflux.yaml and the SQL files. Names
// come from every statement that defines one, even a broken one.
func (p *project) checkConfigNames() {
	p.checkResourceNames()
	for _, t := range p.cfg.Tables {
		if _, ok := p.tables[t.Name]; !ok {
			p.configErrorf(t.Pos, "DF6407", p.nameHint(t.Name, declStream, declView),
				"tables key %q names no stream or view", t.Name)
		}
	}
	for _, a := range p.cfg.Archive.Streams {
		if d, ok := p.tables[a.Name]; !ok || d.kind != declStream {
			p.configErrorf(a.Pos, "DF6503", p.nameHint(a.Name, declStream), "archive.streams key %q names no stream", a.Name)
		}
	}
	p.checkInheritedForever()
	p.checkUnarchived()
}

func (p *project) configErrorf(pos diag.Pos, code, hint, format string, args ...any) {
	p.d.add(diag.SeverityError, "", pos, code, hint, fmt.Sprintf(format, args...))
}

// nameHint suggests a defined name close to name, or lists the names of the given kinds.
func (p *project) nameHint(name string, kinds ...declKind) string {
	names := p.tableNames(kinds...)
	if c, ok := diag.Suggest(name, names); ok {
		return fmt.Sprintf("did you mean %q?", c)
	}
	if len(names) == 0 {
		return "the SQL files define none"
	}
	slices.Sort(names)
	return "the SQL files define " + strings.Join(names, ", ")
}

// tableNames returns the stream and view names of the given kinds.
func (p *project) tableNames(kinds ...declKind) []string {
	var names []string
	for n, d := range p.tables {
		if slices.Contains(kinds, d.kind) {
			names = append(names, n)
		}
	}
	return names
}

// checkResourceNames checks the AWS names derived from each stream and view (DF6110).
// Views are version 1 until validate reads dynaflux.lock.
func (p *project) checkResourceNames() {
	var c catalog.Catalog
	for _, d := range p.decls {
		if !d.defines || d.earlier != nil {
			continue
		}
		switch d.kind {
		case declStream:
			c.Streams = append(c.Streams, catalog.Stream{Name: d.name, Archive: p.archive(d.name)})
		case declView:
			c.Views = append(c.Views, catalog.View{Name: d.name, Versions: []catalog.ViewVersion{{Number: 1}}})
		case declQuery:
		}
	}
	base := len(p.cfg.Prefix) + len(p.cfg.Store)
	for _, r := range p.cfg.Naming().CatalogResources(&c, p.cfg.Runtime.Split == config.SplitStream) {
		if !r.TooLong() {
			continue
		}
		d := p.tables[r.Owner]
		fixed := len(r.Name) - len(r.Owner)
		hint := fmt.Sprintf("shorten the %s name to at most %d characters", kindNoun(d.kind), r.Kind.MaxLen()-fixed)
		if strings.HasPrefix(r.Name, p.cfg.Prefix+p.cfg.Store+"-") {
			hint = fmt.Sprintf("shorten the %s name, store or prefix: together they can have at most %d characters",
				kindNoun(d.kind), r.Kind.MaxLen()-(fixed-base))
		}
		d.st.errorf(d.pos, "DF6110", hint, "the %s name %s has %d characters; the limit is %d",
			r.Kind, r.Name, len(r.Name), r.Kind.MaxLen())
	}
}

func kindNoun(k declKind) string {
	if k == declView {
		return "view"
	}
	return "stream"
}

// checkInheritedForever warns when a stream inherits defaults.log_retention: forever
// (DF6403).
func (p *project) checkInheritedForever() {
	if _, limited := p.cfg.Defaults.LogRetention.Period(); limited {
		return
	}
	var names []string
	for _, d := range p.decls {
		if d.stream != nil && d.stream.inherited {
			names = append(names, d.name)
		}
	}
	if len(names) == 0 {
		return
	}
	subject := "stream " + names[0] + " inherits"
	if len(names) > 1 {
		subject = "streams " + strings.Join(names, ", ") + " inherit"
	}
	p.d.add(diag.SeverityWarning, "", p.cfg.Defaults.LogRetentionPos, "DF6403",
		"keep a short log (the default is 2d) and use the archive as the permanent record; dynaflux explain shows the cost",
		subject+" defaults.log_retention: forever, so every event stays in DynamoDB and the cost of projecting it keeps growing")
}

// checkUnarchived warns about streams with a limited log that aren't archived (DF6504).
func (p *project) checkUnarchived() {
	for _, d := range p.decls {
		if d.stream == nil {
			continue
		}
		s := &d.stream.stream
		period, limited := s.LogRetention.Period()
		if s.Archive != nil || !limited {
			continue
		}
		const hint = "archive the stream, or give it LOG RETENTION FOREVER"
		msg := fmt.Sprintf("stream %s keeps events for %s and isn't archived, so older events are deleted for good",
			d.name, period)
		i := slices.IndexFunc(p.cfg.Archive.Streams, func(a config.ArchiveStream) bool { return a.Name == d.name })
		if i >= 0 {
			p.d.add(diag.SeverityWarning, "", p.cfg.Archive.Streams[i].EnabledPos, "DF6504", hint, msg)
		} else {
			d.st.warnf(d.pos, "DF6504", hint, "%s", msg)
		}
	}
}
