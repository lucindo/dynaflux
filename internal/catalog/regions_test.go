// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
)

func TestCheckRegion(t *testing.T) {
	t.Parallel()
	const fipsHint = "name the region, such as us-east-1, and set AWS_USE_FIPS_ENDPOINT=true to use FIPS endpoints"
	const codeHint = "use a code such as us-east-1"
	tests := []struct {
		region, problem, hint string
	}{
		{"us-east-1", "", ""},
		{"ap-southeast-2", "", ""},
		{"us-gov-west-1", "", ""},
		{"fips-us-east-1", `"fips-us-east-1" is a FIPS pseudo-region`, fipsHint},
		{"us-east-1-fips", `"us-east-1-fips" is a FIPS pseudo-region`, fipsHint},
		{"us-fips-east-1", `"us-fips-east-1" is a FIPS pseudo-region`, fipsHint},
		{"US-EAST-1", `"US-EAST-1" isn't an AWS region code`, codeHint},
		{"local", `"local" isn't an AWS region code`, codeHint},
		{"use-east-1", `"use-east-1" isn't an AWS region code`, codeHint},
		{"", `"" isn't an AWS region code`, codeHint},
	}
	for _, tt := range tests {
		problem, hint := catalog.CheckRegion(tt.region)
		if problem != tt.problem || hint != tt.hint {
			t.Errorf("CheckRegion(%q) = %q, %q; want %q, %q", tt.region, problem, hint, tt.problem, tt.hint)
		}
	}
}
