// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"iter"
	"slices"
)

// StorageFormatVersion is the newest storage format: new streams and view versions get it,
// and the runtime reads every format from 1 up to it. It versions everything stored: key
// and value encodings, aggregate states and the event_id scheme. A stream or view version
// keeps its format for life.
const StorageFormatVersion = 1

// MaxPruneWindowSeconds is the largest runtime.prune_window_seconds: 25 hours.
const MaxPruneWindowSeconds = 90000

// LifetimeMarginSeconds is how much longer than the longest invocation the prune window is,
// for writes that land late and clock skew between hosts.
const LifetimeMarginSeconds = 60

// Catalog is the compiled model of one store, with every default applied. Lists whose order
// carries meaning keep it (columns, keys, parameters, outputs); [Marshal] sorts the others.
type Catalog struct {
	// Store is the store name from dynaflux.yaml.
	Store string `json:"store"`
	// Regions are the store's AWS regions: the Global Table replicas.
	Regions []string `json:"regions,omitempty"`
	// PruneWindowSeconds is runtime.prune_window_seconds: how long the projector keeps a
	// delivery unit's high-water mark in a view row after the unit's progress has passed it.
	PruneWindowSeconds int      `json:"prune_window_seconds"`
	Streams            []Stream `json:"streams,omitempty"`
	Views              []View   `json:"views,omitempty"`
	// Retired are the IDs of the retired view versions, such as page_daily@v1. A projector
	// counts their fences as settled.
	Retired []string `json:"retired,omitempty"`
	Queries []Query  `json:"queries,omitempty"`
}

// Stream returns the stream named name.
func (c *Catalog) Stream(name string) (*Stream, bool) {
	i := slices.IndexFunc(c.Streams, func(s Stream) bool { return s.Name == name })
	if i < 0 {
		return nil, false
	}
	return &c.Streams[i], true
}

// ExactHandoff reports whether region archives every event of stream, local and
// replicated: only that archive can fill a version by the fenced handoff there.
func (c *Catalog) ExactHandoff(region, stream string) bool {
	s, ok := c.Stream(stream)
	return ok && s.Archive != nil && s.Archive.Events == ArchiveEventsAll && slices.Contains(s.Archive.Regions, region)
}

// Versions yields each view version with its ID, such as page_daily@v2, in the order of
// Views.
func (c *Catalog) Versions() iter.Seq2[string, *ViewVersion] {
	return func(yield func(string, *ViewVersion) bool) {
		for i := range c.Views {
			v := &c.Views[i]
			for j := range v.Versions {
				if !yield(VersionID(v.Name, v.Versions[j].Number), &v.Versions[j]) {
					return
				}
			}
		}
	}
}

// Version returns the view version whose ID is id.
func (c *Catalog) Version(id string) (*ViewVersion, bool) {
	for vid, vv := range c.Versions() {
		if vid == id {
			return vv, true
		}
	}
	return nil, false
}

// View returns the view named name.
func (c *Catalog) View(name string) (*View, bool) {
	i := slices.IndexFunc(c.Views, func(v View) bool { return v.Name == name })
	if i < 0 {
		return nil, false
	}
	return &c.Views[i], true
}

// ExposedViews yields each view that a query annotated -- expose: http reads, once, by name,
// with its current version, which such a query reads.
func (c *Catalog) ExposedViews() iter.Seq2[*View, *ViewVersion] {
	return func(yield func(*View, *ViewVersion) bool) {
		var names []string
		for _, q := range c.Queries {
			if q.ExposeHTTP && q.Source.Kind == SourceView {
				names = append(names, q.Source.Name)
			}
		}
		slices.Sort(names)
		for _, name := range slices.Compact(names) {
			v, _ := c.View(name)
			vv, _ := v.Current() // a compiled query reads its view's current version
			if !yield(v, vv) {
				return
			}
		}
	}
}
