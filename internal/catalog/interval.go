// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Unit is the unit of an [Interval]. MONTH, QUARTER and YEAR are calendar arithmetic in
// UTC that clamps to the end of the month.
type Unit string

// Interval units, in their singular spelling.
const (
	UnitSecond  Unit = "SECOND"
	UnitMinute  Unit = "MINUTE"
	UnitHour    Unit = "HOUR"
	UnitDay     Unit = "DAY"
	UnitWeek    Unit = "WEEK"
	UnitMonth   Unit = "MONTH"
	UnitQuarter Unit = "QUARTER"
	UnitYear    Unit = "YEAR"
)

func (u Unit) known() bool {
	switch u {
	case UnitSecond, UnitMinute, UnitHour, UnitDay, UnitWeek, UnitMonth, UnitQuarter, UnitYear:
		return true
	}
	return false
}

// Interval is N units of time. Its text form is "N UNIT", such as "5 MINUTE". N is never
// negative: the direction comes from where the interval is used.
type Interval struct {
	N    int
	Unit Unit
}

// MarshalText implements [encoding.TextMarshaler].
func (i Interval) MarshalText() ([]byte, error) {
	if i.N < 0 || !i.Unit.known() {
		return nil, fmt.Errorf("invalid interval %d %q", i.N, i.Unit)
	}
	return []byte(strconv.Itoa(i.N) + " " + string(i.Unit)), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler] for the form MarshalText writes.
func (i *Interval) UnmarshalText(text []byte) error {
	n, unit, found := strings.Cut(string(text), " ")
	v, err := strconv.Atoi(n)
	parsed := Interval{N: v, Unit: Unit(unit)}
	if !found || err != nil || strconv.Itoa(v) != n || parsed.N < 0 || !parsed.Unit.known() {
		return fmt.Errorf("invalid interval %q", text)
	}
	*i = parsed
	return nil
}

const retentionForever = "FOREVER"

// Retention is a stream's LOG RETENTION: from a day to 100 years, in hours, days, weeks,
// months or years, or forever. Its text form is "FOREVER" or an [Interval]. The zero value
// is invalid, so a retention the compiler forgot to resolve fails to marshal instead of
// keeping events forever.
type Retention struct {
	period  Interval
	forever bool
}

// RetainFor returns a retention of period p.
func RetainFor(p Interval) Retention {
	return Retention{period: p}
}

// RetainForever returns LOG RETENTION FOREVER.
func RetainForever() Retention {
	return Retention{forever: true}
}

// Period returns the retention period, and false for FOREVER.
func (r Retention) Period() (Interval, bool) {
	return r.period, !r.forever
}

// MarshalText implements [encoding.TextMarshaler].
func (r Retention) MarshalText() ([]byte, error) {
	if r.forever {
		return []byte(retentionForever), nil
	}
	if r.period == (Interval{}) {
		return nil, errors.New("log retention is not set")
	}
	if !retentionPeriod(r.period) {
		return nil, fmt.Errorf("invalid log retention %d %s", r.period.N, r.period.Unit)
	}
	return r.period.MarshalText()
}

// UnmarshalText implements [encoding.TextUnmarshaler] for the form MarshalText writes.
func (r *Retention) UnmarshalText(text []byte) error {
	if string(text) == retentionForever {
		*r = RetainForever()
		return nil
	}
	var p Interval
	if err := p.UnmarshalText(text); err != nil {
		return fmt.Errorf("log retention: %w", err)
	}
	if !retentionPeriod(p) {
		return fmt.Errorf("invalid log retention %q", text)
	}
	*r = RetainFor(p)
	return nil
}

// retentionPeriod reports whether LOG RETENTION allows p: at least a day and at most 100
// years of 365.25 days (sql.md §8.6). DynamoDB TTL deletes expired items late, often by
// days, and a cut-off region could expire events before they replicate, so shorter
// windows are refused.
func retentionPeriod(p Interval) bool {
	const (
		hoursPerDay   = 24
		daysPerWeek   = 7
		monthsPerYear = 12
		maxDays       = 36525
		maxYears      = 100
	)
	switch p.Unit {
	case UnitHour:
		return p.N >= hoursPerDay && p.N <= maxDays*hoursPerDay
	case UnitDay:
		return p.N >= 1 && p.N <= maxDays
	case UnitWeek:
		return p.N >= 1 && p.N <= maxDays/daysPerWeek
	case UnitMonth:
		return p.N >= 1 && p.N <= maxYears*monthsPerYear
	case UnitYear:
		return p.N >= 1 && p.N <= maxYears
	case UnitSecond, UnitMinute, UnitQuarter:
	}
	return false
}
