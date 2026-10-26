// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	digitGroup     = 3   // digits between thousands separators
	wholeFrom      = 100 // num rounds numbers this large to whole numbers
	significant    = 3   // num's significant digits below wholeFrom
	cents          = 10  // dollars shows cents below this
	columnGap      = 2
	sectionPadding = 11 // width of the labels of a section's lines
)

// num formats x for reading: whole numbers with thousands separators from 100 up, and 3
// significant digits below, without trailing zeros.
func num(x float64) string {
	if math.Abs(x) >= wholeFrom {
		return whole(x)
	}
	decimals := 0
	if x != 0 {
		decimals = max(0, significant-1-int(math.Floor(math.Log10(math.Abs(x)))))
	}
	s := strconv.FormatFloat(x, 'f', decimals, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// whole formats x rounded to a whole number, with thousands separators. It doesn't convert
// to int64, whose range a count can pass.
func whole(x float64) string {
	return separate(strconv.FormatFloat(math.Round(x), 'f', 0, 64))
}

// grouped formats n with thousands separators.
func grouped(n int64) string {
	return separate(strconv.FormatInt(n, 10))
}

// separate puts thousands separators into the digits of a whole number.
func separate(s string) string {
	sign, digits := "", s
	if d, ok := strings.CutPrefix(s, "-"); ok {
		sign, digits = "-", d
	}
	for i := len(digits) - digitGroup; i > 0; i -= digitGroup {
		digits = digits[:i] + "," + digits[i:]
	}
	return sign + digits
}

func dollars(x float64) string {
	if x < cents {
		return fmt.Sprintf("$%.2f", x)
	}
	return "$" + whole(x)
}

func units(x float64) string {
	return strconv.FormatFloat(x, 'f', 2, 64)
}

// flagValue formats a workload value as a flag takes it.
func flagValue(x float64) string {
	return strconv.FormatFloat(x, 'g', -1, 64)
}

// table aligns columns: the first ones to the left, the others, numbers, to the right.
type table struct {
	left int // how many columns are aligned to the left
	rows [][]string
}

func (t *table) add(cells ...string) {
	t.rows = append(t.rows, cells)
}

// write writes the rows without trailing spaces.
func (t *table) write(b *strings.Builder) {
	var widths []int
	for _, row := range t.rows {
		for i, cell := range row {
			if i == len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], len([]rune(cell)))
		}
	}
	for _, row := range t.rows {
		var line strings.Builder
		for i, cell := range row {
			pad := strings.Repeat(" ", widths[i]-len([]rune(cell)))
			if i > 0 {
				line.WriteString(strings.Repeat(" ", columnGap))
			}
			if i < t.left {
				line.WriteString(cell + pad)
			} else {
				line.WriteString(pad + cell)
			}
		}
		b.WriteString(strings.TrimRight(line.String(), " ") + "\n")
	}
}
