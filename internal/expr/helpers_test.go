// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr_test

import (
	"encoding/hex"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// ty parses a type name of a test table; a typo there is a bug in the test.
func ty(name string) catalog.Type {
	t, err := catalog.ParseType(name)
	if err != nil {
		panic(err)
	}
	return t
}

func lit(typ, value string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpLiteral, Type: ty(typ), Value: value}
}

func col(name, typ string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpColumn, Type: ty(typ), Name: name}
}

func param(name, typ string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpParam, Type: ty(typ), Name: name}
}

func null(typ string) catalog.Expr {
	return catalog.Expr{Op: catalog.OpNull, Type: ty(typ)}
}

func call(op catalog.Op, typ string, args ...catalog.Expr) catalog.Expr {
	return catalog.Expr{Op: op, Type: ty(typ), Args: args}
}

func boolean(b bool) catalog.Expr { return lit("Bool", strconv.FormatBool(b)) }

// render writes v, of type t, in the canonical text of catalog.Expr. A temporal value
// finer than its type's precision shows all six fractional digits, so a bucket that isn't
// rounded down can't pass for one that is.
func render(t catalog.Type, v expr.Value) string {
	if v.IsNull() {
		return "NULL"
	}
	if t.Array {
		t.Array = false
		parts := make([]string, 0, len(v.AsArray()))
		for _, e := range v.AsArray() {
			parts = append(parts, render(t, e))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	switch k := t.Kind; {
	case k == catalog.KindString:
		return strconv.Quote(v.AsString())
	case k >= catalog.KindUInt8 && k <= catalog.KindUInt64:
		return strconv.FormatUint(v.AsUint(), 10)
	case k >= catalog.KindInt8 && k <= catalog.KindInt64:
		return strconv.FormatInt(v.AsInt(), 10)
	case k == catalog.KindFloat64:
		return strconv.FormatFloat(v.AsFloat(), 'g', -1, 64)
	case k == catalog.KindDecimal:
		return renderDecimal(v.AsDecimal(), t.Scale)
	case k == catalog.KindBool:
		return strconv.FormatBool(v.AsBool())
	case k == catalog.KindUUID:
		b := v.AsUUID()
		h := hex.EncodeToString(b[:])
		return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	}
	return renderTime(t, v.AsMicros())
}

func renderDecimal(unscaled *big.Int, scale int) string {
	digits := new(big.Int).Abs(unscaled).String()
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	s := digits[:len(digits)-scale]
	if scale > 0 {
		s += "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		return "-" + s
	}
	return s
}

func renderTime(t catalog.Type, us int64) string {
	const full = "2006-01-02 15:04:05.000000"
	layout := "2006-01-02"
	if t.Kind == catalog.KindDateTime {
		layout = "2006-01-02 15:04:05"
	}
	if t.Kind == catalog.KindDateTime64 {
		layout = "2006-01-02 15:04:05." + strings.Repeat("0", t.Precision)
	}
	tm := time.UnixMicro(us).UTC()
	s := tm.Format(layout)
	if back, err := time.Parse(layout, s); err != nil || back.UnixMicro() != us {
		return tm.Format(full)
	}
	return s
}
