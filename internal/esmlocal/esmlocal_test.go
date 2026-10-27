// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package esmlocal_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/esmlocal"
)

func TestEventFinalInvocation(t *testing.T) {
	t.Parallel()
	got := string(esmlocal.Event("shardId-1", "arn:aws:dynamodb:ddblocal:000000000000:table/t/stream/1"))
	want := `{"Records":[],"shardId":"shardId-1","eventSourceARN":"arn:aws:dynamodb:ddblocal:000000000000:table/t/stream/1",` +
		`"window":{"start":"1970-01-01T00:00:00Z","end":"1970-01-01T00:15:00Z"},` +
		`"state":{},"isFinalInvokeForWindow":true,"isWindowTerminatedEarly":false}`
	if got != want {
		t.Errorf("Event without records:\n got  %s\n want %s", got, want)
	}
}
