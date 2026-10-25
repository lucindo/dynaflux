// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"slices"
	"time"
)

// StorageFormatVersion is the newest storage format: new streams and view versions get it,
// and the runtime reads every format from 1 up to it. It versions everything stored: key
// and value encodings, aggregate states and the event_id scheme. A stream or view version
// keeps its format for life.
const StorageFormatVersion = 1

const lifetimeMargin = 60 * time.Second

// Catalog is the compiled model of one store, with every default applied. Lists whose order
// carries meaning keep it (columns, keys, parameters, outputs); [Marshal] sorts the others.
type Catalog struct {
	// Store is the store name from dynaflux.yaml.
	Store string `json:"store"`
	// Regions are the store's AWS regions: the Global Table replicas.
	Regions []string `json:"regions,omitempty"`
	// TimeoutSeconds is the Lambda timeout. [Catalog.InvocationLifetime] derives from it.
	TimeoutSeconds int      `json:"timeout_seconds"`
	Streams        []Stream `json:"streams,omitempty"`
	Views          []View   `json:"views,omitempty"`
	Queries        []Query  `json:"queries,omitempty"`
}

// InvocationLifetime returns the longest that a runtime function invocation is assumed to
// run: the Lambda timeout plus 60 seconds. The projector relies on it when it prunes
// high-water marks, and explain reports it.
func (c *Catalog) InvocationLifetime() time.Duration {
	return time.Duration(c.TimeoutSeconds)*time.Second + lifetimeMargin
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
