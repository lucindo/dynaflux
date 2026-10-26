// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"slices"
	"strings"
)

// suggest returns the candidate to suggest for an unknown name (sql.md §3.4): one that
// differs only in letter case, or else the one with the smallest edit distance, if that
// is at most 2. Ties go to the candidate that sorts first.
func suggest(s string, candidates []string) (string, bool) {
	const maxDistance = 2
	if s == "" {
		return "", false
	}
	sorted := slices.Sorted(slices.Values(candidates))
	for _, c := range sorted {
		if strings.EqualFold(c, s) && c != s {
			return c, true
		}
	}
	best, bestDist := "", maxDistance+1
	for _, c := range sorted {
		if dist := levenshtein(s, c); dist > 0 && dist < bestDist {
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

// didYouMean returns the hint that suggests a name, or "".
func didYouMean(s string, candidates []string) string {
	if c, ok := suggest(s, candidates); ok {
		return "did you mean " + c + "?"
	}
	return ""
}
