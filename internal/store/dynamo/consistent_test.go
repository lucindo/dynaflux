// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo_test

import (
	"context"
	"iter"
	"slices"
	"testing"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/store"
)

// emptyReads answers every read with no item.
func emptyReads(op string, _ []byte) (int, string) {
	switch op {
	case "GetItem":
		return 200, `{}`
	case "BatchGetItem":
		return 200, `{"Responses":{"t":[]},"UnprocessedKeys":{}}`
	case "Query", "Scan":
		return 200, `{"Count":0,"Items":[]}`
	}
	return 400, errorBody("UnknownOperationException")
}

// misread returns the reads of b whose request didn't carry ConsistentRead as asked.
func misread(t *testing.T, b store.Backend, rec *emulatortest.Recorder) []string {
	t.Helper()
	ctx := t.Context()
	reads := []struct {
		name string
		read func(consistent bool) error
	}{
		{"Get", func(consistent bool) error {
			_, err := b.Get(ctx, "t", key, consistent)
			return err
		}},
		{"BatchGet", func(consistent bool) error {
			_, err := b.BatchGet(ctx, "t", []store.Key{key}, consistent)
			return err
		}},
		{"Query", func(consistent bool) error {
			for _, err := range b.Query(ctx, "t", store.Query{PK: key.PK, Consistent: consistent}) {
				return err
			}
			return nil
		}},
		{"ScanPage", func(consistent bool) error {
			_, _, err := b.ScanPage(ctx, "t", store.Scan{Segments: 1, Consistent: consistent}, nil)
			return err
		}},
	}
	var wrong []string
	for _, consistent := range []bool{false, true} {
		for _, r := range reads {
			before := len(rec.Calls())
			if err := r.read(consistent); err != nil {
				t.Fatalf("%s: %v", r.name, err)
			}
			calls := rec.Calls()[before:]
			if len(calls) != 1 || calls[0].Consistent != consistent {
				wrong = append(wrong, r.name)
			}
		}
	}
	return wrong
}

// DynamoDB Local reads consistently whatever the flag says, so only the request shows a
// dropped ConsistentRead before AWS does.
func TestConsistentReadSent(t *testing.T) {
	t.Parallel()
	rec := emulatortest.NewRecorder(fake(emptyReads))
	if wrong := misread(t, newBackend(rec), rec); wrong != nil {
		t.Errorf("ConsistentRead not as asked in %q", wrong)
	}
}

// The control: a backend that ignores consistent fails the check above.
func TestConsistentReadSentControl(t *testing.T) {
	t.Parallel()
	rec := emulatortest.NewRecorder(fake(emptyReads))
	got := misread(t, alwaysEventual{newBackend(rec)}, rec)
	if want := []string{"Get", "BatchGet", "Query", "ScanPage"}; !slices.Equal(got, want) {
		t.Errorf("a backend that drops ConsistentRead fails the check in %q, want %q", got, want)
	}
}

// alwaysEventual sends every read as eventually consistent.
type alwaysEventual struct{ store.Backend }

func (a alwaysEventual) Get(ctx context.Context, table string, k store.Key, _ bool) (store.Item, error) {
	return a.Backend.Get(ctx, table, k, false)
}

func (a alwaysEventual) BatchGet(ctx context.Context, table string, ks []store.Key, _ bool) ([]store.Item, error) {
	return a.Backend.BatchGet(ctx, table, ks, false)
}

func (a alwaysEventual) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	q.Consistent = false
	return a.Backend.Query(ctx, table, q)
}

func (a alwaysEventual) ScanPage(ctx context.Context, table string, s store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	s.Consistent = false
	return a.Backend.ScanPage(ctx, table, s, start)
}
