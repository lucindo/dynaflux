// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxruntime_test

import (
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/fluxruntime"
)

// A partial failure's log line cuts the rows' errors on a rune boundary, so the line stays
// valid UTF-8.
func TestCut(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 1023)
	tests := []struct {
		s    string
		n    int
		want string
	}{
		{"abc", 3, "abc"},
		{"abcd", 3, "abc …"},
		{long + "é", 1024, long + " …"},
		{"a😀b", 4, "a …"},
		{"é", 1, " …"},
	}
	for _, tt := range tests {
		if got := fluxruntime.Cut(tt.s, tt.n); got != tt.want {
			t.Errorf("cut(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
		}
	}
}
