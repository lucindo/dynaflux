// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package access lets fluxgen and fluxtest build and open the types of package dynaflux,
// which has no exported constructor that takes internal types. Package dynaflux registers
// the functions when it initializes, so they are set in every package that imports it.
package access

import (
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
)

// Funcs are package dynaflux's functions. Each any is one of its types, as the comments
// name: a destination is a pointer for the function to fill.
type Funcs struct {
	InitStore    func(dst any, e *engine.Engine, check func(*catalog.Catalog) error) // dst is a *dynaflux.Store; check is why it can't serve a catalog
	Bind         func(s any, c *engine.Catalog) (*engine.Engine, error)              // s is a *dynaflux.Store; fails if s can't serve c
	InitCatalog  func(dst any, c *engine.Catalog, err error)                         // dst is a *dynaflux.Catalog
	Catalog      func(c any) (*engine.Catalog, error)                                // c is a *dynaflux.Catalog
	InitToken    func(dst any, text string)                                          // dst is a *dynaflux.Token
	ReadRange    func(opts any) (since, until *time.Time, err error)                 // opts is a []dynaflux.ReadOption
	QueryOptions func(opts any) (engine.QueryOptions, error)                         // opts is a []dynaflux.QueryOption
	AppendMode   func(opts any) (engine.AppendMode, error)                           // opts is a []dynaflux.AppendOption
}

var funcs Funcs //nolint:gochecknoglobals // set once, by package dynaflux's init

// Register stores package dynaflux's functions.
func Register(f Funcs) { funcs = f }

// Get returns the functions package dynaflux registered.
func Get() Funcs { return funcs }
