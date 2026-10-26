// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package gotype maps SQL types to the Go types of generated code, for code generation and
// for package fluxgen alike.
package gotype

import (
	"reflect"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/internal/catalog"
)

//nolint:gochecknoglobals // a read-only table, indexed by kind
var scalars = [...]reflect.Type{
	catalog.KindString:     reflect.TypeFor[string](),
	catalog.KindUInt8:      reflect.TypeFor[uint8](),
	catalog.KindUInt16:     reflect.TypeFor[uint16](),
	catalog.KindUInt32:     reflect.TypeFor[uint32](),
	catalog.KindUInt64:     reflect.TypeFor[uint64](),
	catalog.KindInt8:       reflect.TypeFor[int8](),
	catalog.KindInt16:      reflect.TypeFor[int16](),
	catalog.KindInt32:      reflect.TypeFor[int32](),
	catalog.KindInt64:      reflect.TypeFor[int64](),
	catalog.KindFloat64:    reflect.TypeFor[float64](),
	catalog.KindDecimal:    reflect.TypeFor[dynaflux.Decimal](),
	catalog.KindBool:       reflect.TypeFor[bool](),
	catalog.KindUUID:       reflect.TypeFor[dynaflux.UUID](),
	catalog.KindDate:       reflect.TypeFor[time.Time](),
	catalog.KindDateTime:   reflect.TypeFor[time.Time](),
	catalog.KindDateTime64: reflect.TypeFor[time.Time](),
}

// Of returns the Go type of the values of t in generated code.
func Of(t catalog.Type) reflect.Type {
	s := scalars[t.Kind]
	switch {
	case t.Array:
		return reflect.SliceOf(s)
	case t.Nullable:
		return reflect.PointerTo(s)
	}
	return s
}
