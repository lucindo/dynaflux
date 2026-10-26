// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
)

// Inputs returns the model inputs for the stream called name and the views it feeds, from a
// project that validate.Load compiled.
func Inputs(cfg *config.Config, cat *catalog.Catalog, w Workload, name string) (Store, Stream, error) {
	if err := w.check(cat); err != nil {
		return Store{}, Stream{}, err
	}
	s, ok := cat.Stream(name)
	if !ok {
		return Store{}, Stream{}, fmt.Errorf("no stream named %s", name)
	}
	p := newStreamPlan(cfg, cat, w, s)
	return p.store, p.model, nil
}
