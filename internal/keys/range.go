// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/store"
)

// Bound is one end of a range over a sort field.
type Bound struct {
	Value     expr.Value
	Inclusive bool
}

// SKRange is the sort-key condition of a Query (encoding.md §3.5).
type SKRange struct {
	// From and To bound the sort key: sk ≥ From and sk ≤ To. An empty bound is open.
	From, To string
	// ToExclusive makes the upper bound sk < To. From is then open.
	ToExclusive bool
	// Empty reports bounds that no value lies between: the read sends no request.
	Empty bool
	// Loose reports a bound left out because it is longer than a sort key can be. The read
	// may then return any number of items past that bound, on either side, so a LIMIT counts
	// only the items that pass the typed re-check.
	Loose bool
}

// Range returns the sort-key condition for the items whose sort fields start with prefix and
// whose next field, of type t, lies between lo and hi. prefix holds the sort fields bound by
// equality, joined by '#', and is "" when there are none. A nil bound is open.
//
// The condition selects every item in the range. With an exclusive hi and !ToExclusive, it
// may also select the item whose sort key equals To, which exists only when the bounded field
// is the last one and the view has no SPLIT; with Loose, items beyond the left-out bound. The
// caller re-checks the typed predicate either way.
func Range(prefix string, t catalog.Type, lo, hi *Bound) (SKRange, error) {
	loField, err := boundField(t, lo)
	if err != nil {
		return SKRange{}, err
	}
	hiField, err := boundField(t, hi)
	if err != nil {
		return SKRange{}, err
	}
	// Every key under prefix is prefix, '#' and a field of at least one byte.
	if len(prefix)+2 > store.MaxSKLen {
		return SKRange{Empty: true}, nil
	}
	if lo != nil && hi != nil {
		c := strings.Compare(loField, hiField)
		if c > 0 || c == 0 && (!lo.Inclusive || !hi.Inclusive) {
			return SKRange{Empty: true}, nil
		}
	}
	base := ""
	if prefix != "" {
		base = prefix + string(sep)
	}
	var r SKRange
	r.From, r.Loose = lowerBound(base, lo, loField)
	if hi != nil {
		r.To = base + hiField
		if hi.Inclusive {
			r.To += string(above)
		}
		switch {
		case len(r.To) > store.MaxSKLen:
			r.To, r.Loose = "", true
		case !hi.Inclusive && r.From == "":
			r.ToExclusive = true
		}
	}
	if r.To == "" && prefix != "" {
		r.To = prefix + string(above)
	}
	return r, nil
}

// lowerBound returns From, and whether lo was left out for its size. field > lo starts above
// every key that continues lo with '#', since no key holds above.
func lowerBound(base string, lo *Bound, field string) (string, bool) {
	if lo == nil {
		return base, false
	}
	from := base + field
	if !lo.Inclusive {
		from += string(above)
	}
	if len(from) > store.MaxSKLen {
		return base, true
	}
	return from, false
}

func boundField(t catalog.Type, b *Bound) (string, error) {
	if b == nil {
		return "", nil
	}
	f, err := AppendField(nil, t, b.Value)
	return string(f), err
}
