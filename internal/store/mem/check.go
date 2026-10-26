// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package mem

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// DynamoDB numbers (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-data-types-numbers):
// up to 38 significant digits, magnitude from 1E-130 to 9.99…E+125.
const (
	maxDigits = 38
	minExp    = -130
	maxExp    = 125
	// maxExpText bounds a written exponent: past it, no mantissa that fits in an item
	// brings the number back in range.
	maxExpText = 1 << 30
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", store.ErrInvalidRequest, fmt.Sprintf(format, args...))
}

// checkKey checks a lookup key: DynamoDB rejects an oversized one rather than finding nothing.
func checkKey(k store.Key) error {
	if err := checkKeyValue(catalog.AttrPK, k.PK, store.MaxPKLen); err != nil {
		return err
	}
	return checkKeyValue(catalog.AttrSK, k.SK, store.MaxSKLen)
}

func checkKeyValue(name, v string, limit int) error {
	switch {
	case v == "":
		return invalidf("%s is empty", name)
	case !utf8.ValidString(v):
		return invalidf("%s is not valid UTF-8", name)
	case len(v) > limit:
		return invalidf("%s is %d bytes, over %d", name, len(v), limit)
	}
	return nil
}

func checkQuery(q store.Query) error {
	if err := checkKeyValue(catalog.AttrPK, q.PK, store.MaxPKLen); err != nil {
		return err
	}
	for _, bound := range []string{q.From, q.To} {
		if bound == "" {
			continue
		}
		if err := checkKeyValue(catalog.AttrSK+" bound", bound, store.MaxSKLen); err != nil {
			return err
		}
	}
	switch {
	case q.ToExclusive && (q.To == "" || q.From != ""):
		return invalidf("an exclusive upper bound needs To and no From")
	case q.From != "" && q.To != "" && q.From > q.To:
		return invalidf("lower bound %q is above upper bound %q", q.From, q.To)
	case q.Limit < 0 || q.PageSize < 0:
		return invalidf("negative Limit %d or PageSize %d", q.Limit, q.PageSize)
	}
	return nil
}

// checkPut returns the item as DynamoDB stores it, or why DynamoDB rejects the request.
func checkPut(it store.Item, c store.Cond) (store.Item, error) {
	if v, ok := c.Version(); ok && v < 1 {
		return nil, invalidf("condition on version %d; versions start at 1", v)
	}
	out := make(store.Item, len(it))
	for name, v := range it {
		if err := checkName(name); err != nil {
			return nil, err
		}
		nv, err := normalize(v)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", name, err)
		}
		out[name] = nv
	}
	for _, k := range []struct {
		name  string
		limit int
	}{{catalog.AttrPK, store.MaxPKLen}, {catalog.AttrSK, store.MaxSKLen}} {
		v, ok := out[k.name]
		switch {
		case !ok || v.Type != store.S:
			return nil, invalidf("key attribute %s is missing or not a string", k.name)
		case v.Str == "":
			return nil, invalidf("%s is empty", k.name)
		case len(v.Str) > k.limit:
			return nil, fmt.Errorf("%w: %s is %d bytes, over %d", store.ErrTooLarge, k.name, len(v.Str), k.limit)
		}
	}
	if size := store.ItemSize(out); size > store.MaxItemSize {
		return nil, fmt.Errorf("%w: item is up to %d bytes, over %d", store.ErrTooLarge, size, store.MaxItemSize)
	}
	return out, nil
}

func checkName(name string) error {
	if name == "" || !utf8.ValidString(name) {
		return invalidf("attribute name %q is empty or not valid UTF-8", name)
	}
	return nil
}

// normalize returns a deep copy of v in DynamoDB's stored form.
func normalize(v store.Value) (store.Value, error) {
	switch v.Type {
	case store.S:
		if !utf8.ValidString(v.Str) {
			return store.Value{}, invalidf("string is not valid UTF-8")
		}
		return store.Value{Type: store.S, Str: v.Str}, nil
	case store.N:
		n, err := parseNumber(v.Str)
		if err != nil {
			return store.Value{}, err
		}
		return store.Value{Type: store.N, Str: n.String()}, nil
	case store.B:
		return store.Value{Type: store.B, Bin: append([]byte{}, v.Bin...)}, nil
	case store.BOOL:
		return store.Value{Type: store.BOOL, Bool: v.Bool}, nil
	case store.L:
		list := make([]store.Value, len(v.List))
		for i, e := range v.List {
			ne, err := normalize(e)
			if err != nil {
				return store.Value{}, fmt.Errorf("element %d: %w", i, err)
			}
			list[i] = ne
		}
		return store.Value{Type: store.L, List: list}, nil
	case store.M:
		m := make(map[string]store.Value, len(v.Map))
		for name, e := range v.Map {
			if err := checkName(name); err != nil {
				return store.Value{}, err
			}
			ne, err := normalize(e)
			if err != nil {
				return store.Value{}, fmt.Errorf("element %q: %w", name, err)
			}
			m[name] = ne
		}
		return store.Value{Type: store.M, Map: m}, nil
	}
	return store.Value{}, invalidf("type %d is not one Dynaflux writes", v.Type)
}

// number is a parsed DynamoDB number: ±d.ddd × 10^exp, where digits holds the
// significant digits and is empty for zero.
type number struct {
	neg    bool
	digits string
	exp    int
}

// parseNumber accepts -?D+(.D+)?([eE][+-]?D+)? and checks DynamoDB's precision and range.
func parseNumber(s string) (number, error) {
	rest, neg := strings.CutPrefix(s, "-")
	intPart, rest := digitRun(rest)
	valid := intPart != ""
	frac := ""
	if after, ok := strings.CutPrefix(rest, "."); ok {
		frac, rest = digitRun(after)
		valid = valid && frac != ""
	}
	e10 := 0
	if rest != "" {
		var ok bool
		e10, ok = parseExponent(rest)
		valid = valid && ok
	}
	if !valid {
		return number{}, invalidf("%q is not a number", s)
	}
	mant := intPart + frac
	first := strings.IndexFunc(mant, func(r rune) bool { return r != '0' })
	if first < 0 {
		return number{}, nil
	}
	n := number{
		neg:    neg,
		digits: strings.TrimRight(mant[first:], "0"),
		exp:    len(intPart) - 1 - first + e10,
	}
	if len(n.digits) > maxDigits {
		return number{}, invalidf("number %q has %d significant digits, over %d", s, len(n.digits), maxDigits)
	}
	if n.exp < minExp || n.exp > maxExp {
		return number{}, invalidf("number %q is outside 1E%d to 9.99…E+%d", s, minExp, maxExp)
	}
	return n, nil
}

func digitRun(s string) (string, string) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return s[:i], s[i:]
}

// parseExponent parses [eE][+-]?D+, refusing exponents too large to need exact arithmetic.
func parseExponent(s string) (int, bool) {
	if s[0] != 'e' && s[0] != 'E' {
		return 0, false
	}
	e, err := strconv.Atoi(s[1:])
	return e, err == nil && e >= -maxExpText && e <= maxExpText
}

// String is the plain decimal form mem stores: no exponent, no leading zeros, no trailing
// zeros in the fraction, and no negative zero.
func (n number) String() string {
	if n.digits == "" {
		return "0"
	}
	var b strings.Builder
	if n.neg {
		b.WriteByte('-')
	}
	last := len(n.digits) - 1
	switch {
	case n.exp < 0:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -n.exp-1))
		b.WriteString(n.digits)
	case n.exp >= last:
		b.WriteString(n.digits)
		b.WriteString(strings.Repeat("0", n.exp-last))
	default:
		b.WriteString(n.digits[:n.exp+1])
		b.WriteByte('.')
		b.WriteString(n.digits[n.exp+1:])
	}
	return b.String()
}
