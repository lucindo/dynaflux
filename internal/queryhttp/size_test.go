// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp_test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// Lambda returns a synchronous response of at most 6,291,456 bytes, a door's envelope
// included. The handler serves a payload of exactly that size and answers 400 past it.
func TestResultTooLarge(t *testing.T) {
	t.Parallel()
	const limit = 6_291_456
	f := newFixture(t, false)
	// Partitions a, b and c hold 16 events of 380,000 bytes and one more, whose pad is
	// empty in a, fills b's response to the limit, and is a byte longer in c.
	fill := func(k string, last int) {
		for range 16 {
			f.append(t, "blobs", strconv.Quote(k), strconv.Quote(strings.Repeat("x", 380_000)), `"2026-10-01T00:00:00Z"`)
		}
		f.append(t, "blobs", strconv.Quote(k), strconv.Quote(strings.Repeat("x", last)), `"2026-10-01T00:00:00Z"`)
	}
	fill("a", 0)
	out, err := f.invoke(t, directPayload("Blobs", `{"k":"a"}`))
	if err != nil || readDirect(t, out).status != http.StatusOK {
		t.Fatalf("a: %.200s, %v", out, err)
	}
	room := limit - len(out)
	fill("b", room)
	fill("c", room+1)

	out, err = f.invoke(t, directPayload("Blobs", `{"k":"b"}`))
	if err != nil || len(out) != limit || readDirect(t, out).status != http.StatusOK {
		t.Errorf("b: %d bytes, %.200s, %v; want 200 in %d bytes", len(out), out, err, limit)
	}
	out, err = f.invoke(t, directPayload("Blobs", `{"k":"c"}`))
	want := errorJSON(400, "result_too_large", "the response is over Lambda's limit of 6291456 bytes: "+
		"narrow the parameters, or add a LIMIT to the query")
	if err != nil || string(out) != want {
		t.Errorf("c: %.300s, %v\nwant %s", out, err, want)
	}
	// A body past the limit before its last row stops there, with the same answer.
	f.append(t, "blobs", `"c"`, strconv.Quote(strings.Repeat("x", 380_000)), `"2026-10-01T00:00:00Z"`)
	if out, err = f.invoke(t, directPayload("Blobs", `{"k":"c"}`)); err != nil || string(out) != want {
		t.Errorf("c and one more: %.300s, %v\nwant %s", out, err, want)
	}
	// The function URL's envelope escapes the body's quotes, which takes b past the limit.
	got := f.call(t, functionURL, http.MethodPost, "/queries/Blobs", `{"k":"b"}`)
	if got.status != http.StatusBadRequest || got.body != want {
		t.Errorf("b through a function URL: %d %.300s\nwant %s", got.status, got.body, want)
	}
	// The log keeps what an operator looks for: the parameters and the rows read.
	lines := f.logLines(t)
	for i, rows := range []float64{17, 18, 17} {
		if l := lines[2+i]; l["code"] != "result_too_large" || fmt.Sprint(l["params"]) != "[k]" || l["rows"] != rows {
			t.Errorf("log line %v, want the code, the parameters and the %v rows read", l, rows)
		}
	}
}
