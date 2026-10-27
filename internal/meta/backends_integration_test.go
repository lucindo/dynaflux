// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package meta_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/storetest"
)

// backends are the backends every meta test runs on: memBackends, and the same three over
// DynamoDB Local, each on tables of its own.
func backends() []backend {
	return append(memBackends(),
		backend{"dynamo", dynamo},
		backend{"dynamo retried", func(t *testing.T) store.Backend {
			t.Helper()
			return storetest.RetriedWrite(dynamo(t))
		}},
		backend{"dynamo stale reads", func(t *testing.T) store.Backend {
			t.Helper()
			return storetest.StaleReads(dynamo(t), dynamo(t))
		}},
	)
}

func dynamo(t *testing.T) store.Backend {
	t.Helper()
	return emulatortest.Backend(t, emulatortest.Config(t, nil))
}
