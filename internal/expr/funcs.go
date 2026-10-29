// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package expr

import (
	"fmt"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func stringOp(fn func(string) string) evalFunc {
	return unary(func(_ *node, v Value) (Value, error) { return String(fn(v.AsString())), nil })
}

// LowerASCII returns s with its ASCII letters in lower case: LOWER changes no other
// letter (sql.md §6).
func LowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// UpperASCII returns s with its ASCII letters in upper case: UPPER changes no other
// letter (sql.md §6).
func UpperASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'a' <= c && c <= 'z' {
			b[i] = c - ('a' - 'A')
		}
	}
	return string(b)
}

// evalConcat evaluates every operand before it returns NULL for a NULL one, as binary does.
func evalConcat(n *node, f *Frame) (Value, error) {
	var b strings.Builder
	null := false
	for i := range n.args {
		v, err := n.arg(i, f)
		if err != nil {
			return Null(), err
		}
		null = null || v.IsNull()
		b.WriteString(v.AsString())
	}
	if null {
		return Null(), nil
	}
	return String(b.String()), nil
}

// evalLength counts the bytes of a String or the elements of an Array.
func evalLength(_ *node, v Value) (Value, error) {
	if v.kind == KindArray {
		return Uint(uint64(len(v.a))), nil
	}
	return Uint(uint64(len(v.s))), nil
}

const (
	microsPerSecond = 1_000_000
	microsPerMinute = 60 * microsPerSecond
	microsPerHour   = 60 * microsPerMinute
	microsPerDay    = 24 * microsPerHour
	microsPerWeek   = 7 * microsPerDay
	fiveMinutes     = 5
)

// floorTime returns a function that rounds a time down to a multiple of step after the
// epoch: every time in Dynaflux is UTC, so days, hours and minutes have fixed lengths.
// Times are never before 1970 (sql.md §4.3), so integer division rounds down.
func floorTime(step int64) evalFunc {
	return unary(func(_ *node, v Value) (Value, error) { return Micros(v.AsMicros() / step * step), nil })
}

// setupStartOfWeek reads the mode, a literal: weeks start on Sunday for even modes and on
// Monday for odd ones.
func setupStartOfWeek(_ *compiler, n *node, _ *catalog.Expr) error {
	monday := n.args[1].val.AsUint()&1 == 1
	n.eval = unary(func(_ *node, v Value) (Value, error) {
		const daysPerWeek, epochWeekday = 7, 4 // 1970-01-01 was a Thursday
		day := v.AsMicros() / microsPerDay
		back := (day + epochWeekday) % daysPerWeek // days since Sunday
		if monday {
			back = (back + daysPerWeek - 1) % daysPerWeek
		}
		// A week that starts before 1970 starts on 1970-01-01, as in ClickHouse.
		return Micros(max(day-back, 0) * microsPerDay), nil
	})
	return nil
}

func evalStartOfMonth(_ *node, v Value) (Value, error) {
	y, m, _ := time.UnixMicro(v.AsMicros()).UTC().Date()
	return Micros(time.Date(y, m, 1, 0, 0, 0, 0, time.UTC).UnixMicro()), nil
}

func evalYYYYMM(_ *node, v Value) (Value, error) {
	const monthsShift = 100
	y, m, _ := time.UnixMicro(v.AsMicros()).UTC().Date()
	return Uint(uint64(y*monthsShift + int(m))), nil //nolint:gosec // years are 1970 to 2106
}

// AddInterval returns the time us, in microseconds since the epoch, plus iv; a negative
// iv.N subtracts. MONTH, QUARTER and YEAR are calendar arithmetic in UTC that keeps the
// time of day and clamps to the end of the month: 2024-01-31 plus 1 MONTH is 2024-02-29.
func AddInterval(us int64, iv catalog.Interval) (int64, error) {
	const monthsPerQuarter, monthsPerYear = 3, 12
	var step int64
	months := 0
	switch iv.Unit {
	case catalog.UnitSecond:
		step = microsPerSecond
	case catalog.UnitMinute:
		step = microsPerMinute
	case catalog.UnitHour:
		step = microsPerHour
	case catalog.UnitDay:
		step = microsPerDay
	case catalog.UnitWeek:
		step = microsPerWeek
	case catalog.UnitMonth:
		months = 1
	case catalog.UnitQuarter:
		months = monthsPerQuarter
	case catalog.UnitYear:
		months = monthsPerYear
	default:
		return 0, fmt.Errorf("add interval: unknown unit %q", iv.Unit)
	}
	if months == 0 {
		return us + int64(iv.N)*step, nil
	}
	t := time.UnixMicro(us).UTC()
	y, m, d := t.Date()
	// time.Date normalizes a month past its end into the next month, so clamp the day first.
	first := time.Date(y, m+time.Month(iv.N*months), 1, 0, 0, 0, 0, time.UTC)
	y, m, _ = first.Date()
	last := time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
	h, mi, s := t.Clock()
	return time.Date(y, m, min(d, last), h, mi, s, t.Nanosecond(), time.UTC).UnixMicro(), nil
}
