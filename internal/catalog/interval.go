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

// ParseUnit returns the unit that word names in full, singular or plural, in any letter
// case, such as MINUTE, minutes or Day.
func ParseUnit(word string) (Unit, bool) {
	u := Unit(strings.TrimSuffix(strings.ToUpper(word), "S"))
	if !u.known() {
		return "", false
	}
	return u, true
}

// Max returns how many units u there are in 100 years of 365.25 days, the longest
// interval (sql.md §8.1), or 0 for an unknown unit.
func (u Unit) Max() uint64 {
	const (
		years                            = 100
		days                             = 36525
		hoursPerDay                      = 24
		minutesPerHour, secondsPerMinute = 60, 60
		daysPerWeek                      = 7
		quartersPerYear, monthsPerYear   = 4, 12
	)
	switch u {
	case UnitSecond:
		return days * hoursPerDay * minutesPerHour * secondsPerMinute
	case UnitMinute:
		return days * hoursPerDay * minutesPerHour
	case UnitHour:
		return days * hoursPerDay
	case UnitDay:
		return days
	case UnitWeek:
		return days / daysPerWeek
	case UnitMonth:
		return years * monthsPerYear
	case UnitQuarter:
		return years * quartersPerYear
	case UnitYear:
		return years
	}
	return 0
}

func (u Unit) known() bool { return u.Max() > 0 }

// Interval is N units of time. Its text form is "N UNIT", such as "5 MINUTE". N is never
// negative: the direction comes from where the interval is used.
type Interval struct {
	N    int
	Unit Unit
}

// String returns the text form of i.
func (i Interval) String() string {
	return strconv.Itoa(i.N) + " " + string(i.Unit)
}

// MarshalText implements [encoding.TextMarshaler].
func (i Interval) MarshalText() ([]byte, error) {
	if i.N < 0 || !i.Unit.known() {
		return nil, fmt.Errorf("invalid interval %d %q", i.N, i.Unit)
	}
	return []byte(i.String()), nil
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
	const hoursPerDay = 24
	least := uint64(1)
	switch p.Unit {
	case UnitHour:
		least = hoursPerDay
	case UnitDay, UnitWeek, UnitMonth, UnitYear:
	case UnitSecond, UnitMinute, UnitQuarter:
		return false
	}
	n := uint64(max(p.N, 0))
	return n >= least && n <= p.Unit.Max()
}
