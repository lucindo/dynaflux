// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation_test

import (
	"maps"
	"regexp"
	"slices"
	"testing"

	"go.yaml.in/yaml/v4"
)

// A continuation's input carries every field of the input that the states read: JSONata
// reads a missing field as undefined, not as an error, so a continuation without steps would
// never continue again.
func TestBackfillContinuationInput(t *testing.T) {
	t.Parallel()
	for name, regional := range goldenTemplates(t, "regional.yaml") {
		resources := templateResources(t, name, regional)
		machine := value(value(value(resources, "BackfillStateMachine"), "Properties"), "Definition")
		states := value(machine, "States")
		read := map[string]bool{}
		for _, state := range []string{"Step", "Continue"} {
			text, err := yaml.Marshal(value(states, state))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range regexp.MustCompile(`\$states\.input\.(\w+)`).FindAllStringSubmatch(string(text), -1) {
				read[m[1]] = true
			}
		}
		input := value(value(value(states, "Continue"), "Arguments"), "Input")
		var carried []string
		for i := 0; i+1 < len(input.Content); i += 2 {
			carried = append(carried, input.Content[i].Value)
		}
		if want := slices.Sorted(maps.Keys(read)); !slices.Equal(slices.Sorted(slices.Values(carried)), want) {
			t.Errorf("%s: the continuation's input carries %q; the states read %q", name, carried, want)
		}
	}
}
