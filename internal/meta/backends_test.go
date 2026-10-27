// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package meta_test

// backends are the backends every meta test runs on; the integration build adds DynamoDB
// Local's.
func backends() []backend { return memBackends() }
