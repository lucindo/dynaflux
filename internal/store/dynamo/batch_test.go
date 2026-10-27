// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux/internal/store"
)

// partial is a BatchGetItem endpoint that reads at most perCall keys of each request and
// leaves the rest unprocessed, as DynamoDB does past 16 MB or under throttling. It holds an
// item for each key whose sk is in stored, with the bytes 1, 2 in b.
type partial struct {
	mu      sync.Mutex
	perCall int
	stored  []string
	sent    [][]string // the sk of each key, per request
	at      []time.Time
}

func (p *partial) answer(_ string, body []byte) (int, string) {
	var in struct {
		RequestItems map[string]struct {
			Keys []struct {
				PK struct {
					S string `json:"S"`
				} `json:"pk"`
				SK struct {
					S string `json:"S"`
				} `json:"sk"`
			} `json:"Keys"`
		} `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return 400, errorBody("SerializationException")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var sks, items, left []string
	for _, k := range in.RequestItems["t"].Keys {
		sks = append(sks, k.SK.S)
		av := fmt.Sprintf(`{"pk":{"S":%q},"sk":{"S":%q}}`, k.PK.S, k.SK.S)
		switch {
		case len(sks) > p.perCall:
			left = append(left, av)
		case slices.Contains(p.stored, k.SK.S):
			items = append(items, strings.TrimSuffix(av, "}")+`,"b":{"B":"AQI="}}`)
		}
	}
	p.sent = append(p.sent, sks)
	p.at = append(p.at, time.Now())
	return 200, fmt.Sprintf(`{"Responses":{"t":[%s]},"UnprocessedKeys":{"t":{"Keys":[%s]}}}`,
		strings.Join(items, ","), strings.Join(left, ","))
}

func keys(sks ...string) []store.Key {
	ks := make([]store.Key, len(sks))
	for i, sk := range sks {
		ks[i] = store.Key{PK: "p", SK: sk}
	}
	return ks
}

func TestBatchGetUnprocessed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p := &partial{perCall: 2, stored: []string{"a", "c"}}
		got, err := newBackend(fake(p.answer)).BatchGet(t.Context(), "t", keys("a", "b", "a", "c", "b"), true)
		if err != nil {
			t.Fatalf("BatchGet: %v", err)
		}
		var sks []string
		for _, it := range got {
			if it == nil {
				sks = append(sks, "-")
			} else {
				sks = append(sks, it["sk"].Str)
			}
		}
		if want := []string{"a", "-", "a", "c", "-"}; !slices.Equal(sks, want) {
			t.Errorf("BatchGet = %q, want %q", sks, want)
		}
		if want := [][]string{{"a", "b", "c"}, {"c"}}; !slices.EqualFunc(p.sent, want, slices.Equal) {
			t.Errorf("requests = %q, want %q: each distinct key once, then the unprocessed one", p.sent, want)
		}
		if wait := p.at[1].Sub(p.at[0]); wait >= 25*time.Millisecond {
			t.Errorf("unprocessed keys sent again after %v, want under 25ms", wait)
		}
		got[0]["sk"] = str("changed")
		got[0]["b"].Bin[0] = 9
		if got[2]["sk"].Str != "a" || got[2]["b"].Bin[0] != 1 {
			t.Error("the items of a repeated key share storage")
		}
	})
}

// Backoff between resends grows, and the context bounds it.
func TestBatchGetUnprocessedUntilDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p := &partial{perCall: 1}
		var sks []string
		for i := range store.MaxBatchGetKeys {
			sks = append(sks, strconv.Itoa(i))
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		_, err := newBackend(fake(p.answer)).BatchGet(ctx, "t", keys(sks...), false)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("BatchGet = %v, want context.DeadlineExceeded", err)
		}
		if last := p.at[len(p.at)-1]; last.After(deadline) {
			t.Errorf("a request was sent %v after the deadline", last.Sub(deadline))
		}
		// Each wait is below 25 ms, 50 ms, …, 800 ms, then 1 s, so seven waits fit in 3 s.
		if n := len(p.sent); n < 8 {
			t.Errorf("%d requests in 3 s, want at least 8", n)
		}
	})
}

// A response that processes no key would make the loop spin.
func TestBatchGetNoProgress(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		p := &partial{perCall: 0}
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute) // ends a loop that spins
		defer cancel()
		_, err := newBackend(fake(p.answer)).BatchGet(ctx, "t", keys("a"), true)
		if err == nil || err.Error() != "dynamo: batch get from t: a response processed no key" || len(p.sent) != 1 {
			t.Errorf("BatchGet = %v after %d requests, want the no-progress error after 1", err, len(p.sent))
		}
	})
}
