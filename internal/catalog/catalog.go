// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import "slices"

// StorageFormatVersion is the newest storage format: new streams and view versions get it,
// and the runtime reads every format from 1 up to it. It versions everything stored: key
// and value encodings, aggregate states and the event_id scheme. A stream or view version
// keeps its format for life.
const StorageFormatVersion = 1

// MaxPruneWindowSeconds is the largest runtime.prune_window_seconds: 25 hours.
const MaxPruneWindowSeconds = 90000

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
	Queries            []Query  `json:"queries,omitempty"`
}

// Stream returns the stream named name.
func (c *Catalog) Stream(name string) (*Stream, bool) {
	i := slices.IndexFunc(c.Streams, func(s Stream) bool { return s.Name == name })
	if i < 0 {
		return nil, false
	}
	return &c.Streams[i], true
}

// View returns the view named name.
func (c *Catalog) View(name string) (*View, bool) {
	i := slices.IndexFunc(c.Views, func(v View) bool { return v.Name == name })
	if i < 0 {
		return nil, false
	}
	return &c.Views[i], true
}
