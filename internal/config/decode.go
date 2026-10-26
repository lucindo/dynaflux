// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/diag"
)

const (
	lower  = "abcdefghijklmnopqrstuvwxyz"
	digits = "0123456789"

	hintMapping = "write key: value lines indented below the key, or {key: value} on one line"
)

// decoder walks the node tree by hand instead of decoding into structs with
// yaml.WithKnownFields: the library's errors carry neither a DF6xxx code nor the keys
// allowed where an unknown key is, and it accepts spellings the spec rejects.
type decoder struct {
	diags []diag.Diagnostic
}

func (d *decoder) errorf(p diag.Pos, code, hint, format string, args ...any) {
	d.diags = append(d.diags, diag.Diagnostic{
		Pos: p, Severity: diag.SeverityError, Code: code, Message: fmt.Sprintf(format, args...), Hint: hint,
	})
}

func (d *decoder) warnf(p diag.Pos, code, hint, format string, args ...any) {
	d.diags = append(d.diags, diag.Diagnostic{
		Pos: p, Severity: diag.SeverityWarning, Code: code, Message: fmt.Sprintf(format, args...), Hint: hint,
	})
}

func pos(line, col int) diag.Pos {
	return diag.Pos{File: FileName, Line: line, Col: col}
}

func at(n *yaml.Node) diag.Pos {
	return pos(n.Line, n.Column)
}

// pairs yields the keys and values of mapping n; nothing for a nil n.
func pairs(n *yaml.Node) iter.Seq2[*yaml.Node, *yaml.Node] {
	return func(yield func(k, v *yaml.Node) bool) {
		if n == nil {
			return
		}
		for i := 0; i+1 < len(n.Content); i += 2 {
			if !yield(n.Content[i], n.Content[i+1]) {
				return
			}
		}
	}
}

func isString(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Tag == "!!str"
}

func isNull(n *yaml.Node) bool {
	return n == nil || n.Kind == yaml.ScalarNode && n.Tag == "!!null"
}

// plain reports whether n is a scalar that is neither quoted nor a block scalar.
func plain(n *yaml.Node) bool {
	const notPlain = yaml.DoubleQuotedStyle | yaml.SingleQuotedStyle | yaml.LiteralStyle | yaml.FoldedStyle
	return n.Kind == yaml.ScalarNode && n.Style&notPlain == 0
}

// describe shows the value of n in a message.
func describe(n *yaml.Node) string {
	switch {
	case n.Kind == yaml.MappingNode:
		return "a mapping"
	case n.Kind == yaml.SequenceNode:
		return "a list"
	case isNull(n):
		return "null"
	case n.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0:
		return "a block string"
	case n.Style&yaml.DoubleQuotedStyle != 0:
		return strconv.Quote(n.Value)
	case n.Style&yaml.SingleQuotedStyle != 0:
		return "'" + n.Value + "'"
	}
	return n.Value
}

// object is a YAML mapping whose keys are known. Null values count as not set, so they
// are absent from vals.
type object struct {
	path string   // dotted path of the mapping; empty at the top level
	pos  diag.Pos // where a missing key points
	vals map[string]*yaml.Node
}

func (o object) key(k string) string {
	if o.path == "" {
		return k
	}
	return o.path + "." + k
}

func (o object) has(k string) bool {
	return o.vals[k] != nil
}

// at returns the position of the value of k, or of the mapping when k isn't set.
func (o object) at(k string) diag.Pos {
	if n := o.vals[k]; n != nil {
		return at(n)
	}
	return o.pos
}

// object reads n, the value at path, as a mapping with the given keys. A nil or null n is a
// mapping with nothing set, whose missing keys point at missing. It returns false when n
// isn't a mapping.
func (d *decoder) object(path string, n *yaml.Node, missing diag.Pos, keys ...string) (object, bool) {
	o := object{path: path, pos: missing, vals: map[string]*yaml.Node{}}
	if isNull(n) {
		return o, true
	}
	if n.Kind != yaml.MappingNode {
		d.typeError(path, n, "a mapping", hintMapping)
		return o, false
	}
	o.pos = at(n)
	allowed := "the keys allowed here are " + strings.Join(keys, ", ")
	for k, v := range pairs(n) {
		if !d.scalarKey(path, k, allowed) {
			continue
		}
		if !slices.Contains(keys, k.Value) {
			hint := allowed
			if s, ok := suggest(k.Value, keys); ok {
				hint = fmt.Sprintf("did you mean %q?", s)
			}
			d.errorf(at(k), "DF6005", hint, "unknown key %q", o.key(k.Value))
			continue
		}
		if !isNull(v) {
			o.vals[k.Value] = v
		}
	}
	return o, true
}

// scalarKey reports whether k, a key of the mapping at path, is a scalar. YAML also allows a
// list or a mapping as a key, which is DF6007.
func (d *decoder) scalarKey(path string, k *yaml.Node, hint string) bool {
	if k.Kind == yaml.ScalarNode {
		return true
	}
	what := "a key of " + path
	if path == "" {
		what = "a top-level key"
	}
	d.typeError(what, k, "a string", hint)
	return false
}

// mapping returns the mapping at k, whose keys are names, or nil when k isn't set or isn't
// a mapping.
func (d *decoder) mapping(o object, k string) *yaml.Node {
	n := o.vals[k]
	if n != nil && n.Kind != yaml.MappingNode {
		d.typeError(o.key(k), n, "a mapping", hintMapping)
		return nil
	}
	return n
}

func (d *decoder) typeError(what string, n *yaml.Node, want, hint string) {
	d.errorf(at(n), "DF6007", hint, "%s is %s; it must be %s", what, describe(n), want)
}

// require reports DF6008 when k isn't set.
func (d *decoder) require(o object, k, example string) {
	if !o.has(k) {
		d.errorf(o.pos, "DF6008", "for example "+k+": "+example, "missing required key %s", o.key(k))
	}
}

// str returns the string at k. It returns false when k isn't set or isn't a string; hint is
// the expected form, for a value of another type.
func (d *decoder) str(o object, k, hint string) (string, bool) {
	n := o.vals[k]
	if n == nil {
		return "", false
	}
	if !isString(n) {
		d.typeError(o.key(k), n, "a string", hint)
		return "", false
	}
	return n.Value, true
}

func (d *decoder) boolean(o object, k string, dst *bool) {
	n := o.vals[k]
	if n == nil {
		return
	}
	if !plain(n) || n.Value != "true" && n.Value != "false" {
		d.typeError(o.key(k), n, "true or false", "write true or false, unquoted")
		return
	}
	*dst = n.Value == "true"
}

// bounds is the range of an integer key.
type bounds struct {
	min, max int
	hint     string // where the range comes from
}

func (b bounds) String() string {
	if b.max == math.MaxInt {
		return fmt.Sprintf("at least %d", b.min)
	}
	return fmt.Sprintf("between %d and %d", b.min, b.max)
}

// integer sets dst to the integer at k and returns true when k is set and in range.
func (d *decoder) integer(o object, k string, dst *int, b bounds) bool {
	n := o.vals[k]
	if n == nil {
		return false
	}
	if !plain(n) || !isDecimal(n.Value) {
		d.typeError(o.key(k), n, "a whole number", "write plain decimal digits, unquoted")
		return false
	}
	v, err := strconv.Atoi(n.Value)
	switch {
	case err != nil: // isDecimal leaves only overflow
		d.errorf(at(n), "DF6010", b.hint, "%s is %s; it doesn't fit in 64 bits", o.key(k), n.Value)
		return false
	case v < b.min || v > b.max:
		d.errorf(at(n), "DF6010", b.hint, "%s is %d; it must be %s", o.key(k), v, b)
		return false
	}
	*dst = v
	return true
}

// isDecimal reports whether s is an integer in plain decimal digits, with an optional
// leading "-" and no leading zeros.
func isDecimal(s string) bool {
	s = strings.TrimPrefix(s, "-")
	return s == "0" || (s != "" && s[0] != '0' && consists(s, digits))
}

// enum sets dst to the value at k and returns true when k is set to one of allowed.
func enum[T ~string](d *decoder, o object, k string, dst *T, allowed ...T) bool {
	hint := "use " + orList(allowed)
	s, ok := d.str(o, k, hint)
	if !ok {
		return false
	}
	if !slices.Contains(allowed, T(s)) {
		d.errorf(o.at(k), "DF6009", hint, "%s is %q, which isn't an allowed value", o.key(k), s)
		return false
	}
	*dst = T(s)
	return true
}

// list returns the string entries of the list at k, and reports the others. withDefault
// tells whether removing k is a fix for an empty list.
func (d *decoder) list(o object, k, example string, withDefault bool) []*yaml.Node {
	n := o.vals[k]
	if n == nil {
		return nil
	}
	if n.Kind != yaml.SequenceNode {
		d.typeError(o.key(k), n, "a list", "for example "+k+": "+example)
		return nil
	}
	if len(n.Content) == 0 {
		hint := "add at least one entry, such as " + k + ": " + example
		if withDefault {
			hint += ", or remove " + o.key(k) + " to use its default"
		}
		d.errorf(at(n), "DF6011", hint, "%s is empty", o.key(k))
		return nil
	}
	var entries []*yaml.Node
	for _, e := range n.Content {
		if !isString(e) {
			d.typeError("an entry of "+o.key(k), e, "a string", "for example "+k+": "+example)
			continue
		}
		entries = append(entries, e)
	}
	return entries
}

// repeated reports DF6012 when v, from entry n of list what, was seen before.
func (d *decoder) repeated(seen map[string]bool, what, v string, n *yaml.Node) bool {
	if seen[v] {
		d.errorf(at(n), "DF6012", "remove the repeat", "%s lists %s twice", what, v)
		return true
	}
	seen[v] = true
	return false
}

// consists reports whether every character of s is in set.
func consists(s, set string) bool {
	return !strings.ContainsFunc(s, func(r rune) bool { return !strings.ContainsRune(set, r) })
}

// orList writes vs as "a, b or c".
func orList[T ~string](vs []T) string {
	var b strings.Builder
	for i, v := range vs {
		switch {
		case i == 0:
		case i == len(vs)-1:
			b.WriteString(" or ")
		default:
			b.WriteString(", ")
		}
		b.WriteString(string(v))
	}
	return b.String()
}

// suggest returns the candidate closest to s, the way the SQL diagnostics suggest names:
// one that differs only in letter case, or else the one with the smallest edit distance, if
// that is at most 2. Ties go to the candidate that sorts first.
func suggest(s string, candidates []string) (string, bool) {
	const maxDistance = 2
	if s == "" {
		// An empty name is within 2 edits of every short candidate.
		return "", false
	}
	best, bestDist := "", maxDistance+1
	for _, c := range slices.Sorted(slices.Values(candidates)) {
		if strings.EqualFold(c, s) {
			return c, true
		}
		if dist := levenshtein(s, c); dist < bestDist {
			best, bestDist = c, dist
		}
	}
	return best, best != ""
}

// levenshtein is the edit distance between a and b, over code points.
func levenshtein(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := range ra {
		cur := make([]int, len(rb)+1)
		cur[0] = i + 1
		for j := range rb {
			cost := 1
			if ra[i] == rb[j] {
				cost = 0
			}
			cur[j+1] = min(prev[j+1]+1, cur[j]+1, prev[j]+cost)
		}
		prev = cur
	}
	return prev[len(rb)]
}
