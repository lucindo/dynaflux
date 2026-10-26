// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store

// Item size rules (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html).
const (
	// numberOverhead is one byte more than the documented "approximately" one, so that
	// ItemSize stays an upper bound.
	numberOverhead    = 2
	containerOverhead = 3
	elementOverhead   = 1
	boolSize          = 1
)

// ItemSize is an upper bound on the size DynamoDB counts for an item: UTF-8 names and
// values, raw bytes for B, and ceil(significant digits / 2) + 2 bytes for a number.
func ItemSize(it Item) int {
	n := 0
	for name, v := range it {
		n += len(name) + valueSize(v)
	}
	return n
}

func valueSize(v Value) int {
	switch v.Type {
	case S:
		return len(v.Str)
	case N:
		return (significantDigits(v.Str)+1)/2 + numberOverhead
	case B:
		return len(v.Bin)
	case BOOL:
		return boolSize
	case L:
		n := containerOverhead
		for _, e := range v.List {
			n += valueSize(e) + elementOverhead
		}
		return n
	case M:
		n := containerOverhead
		for name, e := range v.Map {
			n += len(name) + valueSize(e) + elementOverhead
		}
		return n
	}
	return 0
}

// significantDigits counts the digits of a number's mantissa from its first to its last
// nonzero digit: DynamoDB trims leading and trailing zeros.
func significantDigits(num string) int {
	first, last, pos := -1, -1, 0
	for i := range len(num) {
		c := num[i]
		if c == 'e' || c == 'E' {
			break
		}
		if c < '0' || c > '9' {
			continue
		}
		if c != '0' {
			if first < 0 {
				first = pos
			}
			last = pos
		}
		pos++
	}
	if first < 0 {
		return 0
	}
	return last - first + 1
}
