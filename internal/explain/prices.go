// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package explain

// us-east-1 list prices in dollars, as testdata/costs.py has them; the parity test pins
// them, so changing one means regenerating testdata/reference.json from an edited costs.py.
const (
	wruPrice       = 0.625 / 1e6 // on-demand write unit; a replicated write costs the same
	rruPrice       = 0.125 / 1e6 // on-demand read unit
	wcuHour        = 0.00065     // provisioned write unit-hour
	rcuHour        = 0.00013     // provisioned read unit-hour
	storageGBMonth = 0.25        // Standard table class
	lambdaRequest  = 0.20 / 1e6
	lambdaGBsARM64 = 0.0000133334
	lambdaGBsX86   = 0.0000166667 // not in costs.py; from the Lambda pricing page
	firehoseGB     = 0.045        // delivery to Apache Iceberg tables
	s3GBMonth      = 0.023
	// Between regions, from a US region; Asia costs about 0.09. AWS now says global tables
	// don't pay it for replication.
	transferGB = 0.02
)
