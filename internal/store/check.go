// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
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
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}

// CheckKey returns [ErrInvalidRequest] for a pk or sk that is empty, not valid UTF-8, or
// over [MaxPKLen] or [MaxSKLen] bytes. Backends check before sending: DynamoDB rejects an
// oversized key rather than finding nothing, and the SDK sends an invalid UTF-8 byte as
// U+FFFD without an error, so the request would name another key.
func CheckKey(k Key) error {
	if err := checkKeyValue(catalog.AttrPK, k.PK, MaxPKLen); err != nil {
		return err
	}
	return checkKeyValue(catalog.AttrSK, k.SK, MaxSKLen)
}

// CheckKeys checks the keys of a BatchGet: each as [CheckKey] does, and from 0 to
// [MaxBatchGetKeys] of them. A BatchGet of no keys sends nothing.
func CheckKeys(ks []Key) error {
	if len(ks) > MaxBatchGetKeys {
		return invalidf("%d keys, over %d", len(ks), MaxBatchGetKeys)
	}
	for i, k := range ks {
		if err := CheckKey(k); err != nil {
			return fmt.Errorf("key %d: %w", i, err)
		}
	}
	return nil
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

// CheckQuery returns [ErrInvalidRequest] for a PK, or a nonempty From or To, that
// [CheckKey] would refuse as pk or sk; crossed bounds; ToExclusive without To or with From
// set; or a negative Limit or PageSize.
func CheckQuery(q Query) error {
	if err := checkKeyValue(catalog.AttrPK, q.PK, MaxPKLen); err != nil {
		return err
	}
	for _, bound := range []string{q.From, q.To} {
		if bound == "" {
			continue
		}
		if err := checkKeyValue(catalog.AttrSK+" bound", bound, MaxSKLen); err != nil {
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

// CheckScan returns [ErrInvalidRequest] for Segments outside 1 to [MaxSegments], a Segment
// outside 0 to Segments-1, a negative PageSize, or a start key that [CheckKey] refuses.
func CheckScan(s Scan, start *Key) error {
	switch {
	case s.Segments < 1 || s.Segments > MaxSegments:
		return invalidf("%d segments, want 1 to %d", s.Segments, MaxSegments)
	case s.Segment < 0 || s.Segment >= s.Segments:
		return invalidf("segment %d of %d", s.Segment, s.Segments)
	case s.PageSize < 0:
		return invalidf("negative PageSize %d", s.PageSize)
	case start != nil:
		return CheckKey(*start)
	}
	return nil
}

// CheckPut returns why DynamoDB would reject a Put of it under c: [ErrInvalidRequest] for a
// name, value (the zero [Type] too), number or key it refuses, or a version below 1;
// [ErrTooLarge] for a key over its length limit or an item over [MaxItemSize] by [ItemSize].
func CheckPut(it Item, c Cond) error {
	_, err := Stored(it, c)
	return err
}

// Stored returns the deep copy of it that the in-memory backend stores, every number in
// DynamoDB's normal form, or [CheckPut]'s error.
func Stored(it Item, c Cond) (Item, error) {
	if v, ok := c.Version(); ok && v < 1 {
		return nil, invalidf("condition on version %d; versions start at 1", v)
	}
	out := make(Item, len(it))
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
	}{{catalog.AttrPK, MaxPKLen}, {catalog.AttrSK, MaxSKLen}} {
		v, ok := out[k.name]
		switch {
		case !ok || v.Type != S:
			return nil, invalidf("key attribute %s is missing or not a string", k.name)
		case v.Str == "":
			return nil, invalidf("%s is empty", k.name)
		case len(v.Str) > k.limit:
			return nil, fmt.Errorf("%w: %s is %d bytes, over %d", ErrTooLarge, k.name, len(v.Str), k.limit)
		}
	}
	if size := ItemSize(out); size > MaxItemSize {
		return nil, fmt.Errorf("%w: item is up to %d bytes, over %d", ErrTooLarge, size, MaxItemSize)
	}
	return out, nil
}

// CheckTransact returns why DynamoDB would reject a transaction of puts:
// [ErrInvalidRequest] for no puts, more than [MaxTransactItems], two puts on one item (AWS
// rejects them; DynamoDB Local doesn't), or a put [CheckPut] refuses as invalid;
// [ErrTooLarge] for a put CheckPut refuses as too large, or puts over [MaxTransactSize]
// together.
func CheckTransact(puts []TxPut) error {
	_, err := StoredTransact(puts)
	return err
}

// StoredTransact returns what [Stored] returns for each put, or [CheckTransact]'s error.
func StoredTransact(puts []TxPut) ([]Item, error) {
	if len(puts) == 0 || len(puts) > MaxTransactItems {
		return nil, invalidf("%d puts, want 1 to %d", len(puts), MaxTransactItems)
	}
	type target struct {
		table string
		key   Key
	}
	out := make([]Item, len(puts))
	seen := make(map[target]int, len(puts))
	size := 0
	for i, p := range puts {
		it, err := Stored(p.Item, p.Cond)
		if err != nil {
			return nil, fmt.Errorf("put %d: %w", i, err)
		}
		t := target{p.Table, KeyOf(it)}
		if j, dup := seen[t]; dup {
			return nil, invalidf("puts %d and %d write one item", j, i)
		}
		seen[t] = i
		out[i] = it
		size += ItemSize(it)
	}
	if size > MaxTransactSize {
		return nil, fmt.Errorf("%w: puts are up to %d bytes together, over %d", ErrTooLarge, size, MaxTransactSize)
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
func normalize(v Value) (Value, error) {
	switch v.Type {
	case S:
		if !utf8.ValidString(v.Str) {
			return Value{}, invalidf("string is not valid UTF-8")
		}
		return Value{Type: S, Str: v.Str}, nil
	case N:
		n, err := parseNumber(v.Str)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: N, Str: n.String()}, nil
	case B:
		return Value{Type: B, Bin: append([]byte{}, v.Bin...)}, nil
	case BOOL:
		return Value{Type: BOOL, Bool: v.Bool}, nil
	case L:
		list := make([]Value, len(v.List))
		for i, e := range v.List {
			ne, err := normalize(e)
			if err != nil {
				return Value{}, fmt.Errorf("element %d: %w", i, err)
			}
			list[i] = ne
		}
		return Value{Type: L, List: list}, nil
	case M:
		m := make(map[string]Value, len(v.Map))
		for name, e := range v.Map {
			if err := checkName(name); err != nil {
				return Value{}, err
			}
			ne, err := normalize(e)
			if err != nil {
				return Value{}, fmt.Errorf("element %q: %w", name, err)
			}
			m[name] = ne
		}
		return Value{Type: M, Map: m}, nil
	}
	return Value{}, invalidf("type %d is not one Dynaflux writes", v.Type)
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

// String is DynamoDB's normal form of n: no exponent, no leading zeros, no trailing zeros
// in the fraction, and no negative zero.
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
