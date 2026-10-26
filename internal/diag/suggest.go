// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package diag

import (
	"slices"
	"strings"
)

// Suggest returns the candidate to suggest for the unknown name: one that
// differs only in letter case, or else the one with the smallest edit distance over code
// points, if that is at most 2. Ties go to the candidate that sorts first. A candidate
// equal to name is never suggested, and an empty name gets no suggestion: it is within 2
// edits of every short candidate.
func Suggest(name string, candidates []string) (string, bool) {
	const maxDistance = 2
	if name == "" {
		return "", false
	}
	sorted := slices.Sorted(slices.Values(candidates))
	for _, c := range sorted {
		if strings.EqualFold(c, name) && c != name {
			return c, true
		}
	}
	best, bestDist := "", maxDistance+1
	for _, c := range sorted {
		if dist := levenshtein(name, c); dist > 0 && dist < bestDist {
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
