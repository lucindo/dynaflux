// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
)

// ErrWaitTimeout is a wait whose timeout passed before every receipt it polls was stored.
var ErrWaitTimeout = errors.New("wait for an append timed out")

// MaxWaitEvents bounds the events of a token that a wait polls: a token is unauthenticated
// text, and every poll reads its missing receipts.
const MaxWaitEvents = 1000

// Wait is a query's wait for the events of a token (ADR 0015).
type Wait struct {
	Token   string // the token's text; "" waits for nothing
	Timeout time.Duration
}

// tokenFormat is the first byte of a token's binary form.
const tokenFormat = 1

// The flags of a token's event.
const (
	flagReceipt byte = 1 << iota // the stored event has _rcpt
	flagForeign                  // another region stored it
	flagsKnown  = flagReceipt | flagForeign
)

// token is a token decoded.
type token struct {
	region, stream string
	events         []tokenEvent
}

type tokenEvent struct {
	id           [16]byte // the event_id's bytes
	ingestMicros int64    // the stored ingest_time
	flags        byte
}

// receipt returns the key of ev's receipt.
func (ev tokenEvent) receipt() meta.ReceiptKey {
	return meta.ReceiptKey{EventID: keys.EventIDText(ev.id), It: ev.ingestMicros}
}

// tokenEvent returns how a token names stored, an event of a stream table.
func (e *Engine) tokenEvent(stored store.Item) (tokenEvent, error) {
	_, id, err := keys.ParseStreamSK(stored[catalog.AttrSK].Str)
	if err != nil {
		return tokenEvent{}, err
	}
	a, ok := stored[catalog.AttrIngestTime]
	ingest, err := keys.DecodeValue(catalog.EventTimeType(), a, ok)
	if err != nil {
		return tokenEvent{}, fmt.Errorf("event %s: ingest_time: %w", id, err)
	}
	var flags byte
	if rcpt := stored[catalog.AttrReceipt]; rcpt.Type == store.BOOL && rcpt.Bool {
		flags |= flagReceipt
	}
	if stored[catalog.AttrOriginRegion].Str != e.region {
		flags |= flagForeign
	}
	b, _ := keys.EventIDBytes(id) // ParseStreamSK checked it
	return tokenEvent{id: b, ingestMicros: ingest.AsMicros(), flags: flags}, nil
}

// Token returns the text of a token that names the events of res, an Append's results for
// stream, that are stored: by the append, or before it for a duplicate. It returns "" when
// there are none. It leaves out the partition key, which holds the event's PRIMARY KEY
// values: a token can reach clients and logs.
//
// The text is base64url of the format byte, the engine's region and the stream, then for
// each event its event_id's 16 bytes, its ingest_time in microseconds as stored, and its
// flags. The first event's ingest_time is a uvarint, and each next one a varint of the
// difference from the one before: an append's events share it, or nearly, but an event
// found stored can be older.
func (e *Engine) Token(stream string, res []Appended) string {
	t := &token{region: e.region, stream: stream}
	for _, r := range res {
		if r.Stored != nil {
			t.events = append(t.events, r.token)
		}
	}
	if len(t.events) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(t.binary())
}

// binary returns t's binary form.
func (t *token) binary() []byte {
	b := appendText([]byte{tokenFormat}, t.region)
	b = appendText(b, t.stream)
	for i, ev := range t.events {
		b = append(b, ev.id[:]...)
		if i == 0 {
			b = binary.AppendUvarint(b, uint64(ev.ingestMicros)) //nolint:gosec // an ingest_time is from 1970 on
		} else {
			b = binary.AppendVarint(b, ev.ingestMicros-t.events[i-1].ingestMicros)
		}
		b = append(b, ev.flags)
	}
	return b
}

func appendText(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

var errMalformed = errors.New("malformed token: not the text of a token that an append returned")

// CheckToken returns an error unless text is "" or the text of a token that [Engine.Token]
// returned. It checks the form only: a token holds no secret.
func CheckToken(text string) error {
	_, err := decodeToken(text)
	return err
}

// decodeToken decodes the text of a token, nil for "". It accepts only what Token writes:
// text that the decoded token encodes back to.
func decodeToken(text string) (*token, error) {
	if text == "" {
		return nil, nil //nolint:nilnil // the zero token names no event
	}
	b, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil || len(b) == 0 || b[0] != tokenFormat {
		return nil, errMalformed
	}
	d := &tokenReader{b: b[1:]}
	t := &token{region: d.text(), stream: d.text()}
	for len(d.b) > 0 && !d.bad {
		var ev tokenEvent
		copy(ev.id[:], d.next(len(ev.id)))
		if len(t.events) == 0 {
			ev.ingestMicros = d.uvarint()
		} else {
			ev.ingestMicros = t.events[len(t.events)-1].ingestMicros + d.varint()
		}
		ev.flags = d.next(1)[0]
		if d.bad || ev.ingestMicros < 0 || ev.ingestMicros >= catalog.TimeLimitMicros || ev.flags&^flagsKnown != 0 {
			return nil, errMalformed
		}
		t.events = append(t.events, ev)
	}
	if d.bad || t.region == "" || t.stream == "" || len(t.events) == 0 || !bytes.Equal(t.binary(), b) {
		return nil, errMalformed
	}
	return t, nil
}

// tokenReader reads the fields of a token's binary form. bad is set at the first one that
// runs past the end, and each read after it returns zeros.
type tokenReader struct {
	b   []byte
	bad bool
}

// next returns the next n bytes.
func (r *tokenReader) next(n int) []byte {
	if r.bad || n > len(r.b) {
		r.bad, r.b = true, nil
		return make([]byte, n)
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *tokenReader) text() string {
	n := r.uvarint()
	if n > int64(len(r.b)) {
		r.bad, r.b = true, nil
		return ""
	}
	return string(r.next(int(n)))
}

// uvarint reads a uvarint of at most 63 bits.
func (r *tokenReader) uvarint() int64 {
	n, w := binary.Uvarint(r.b)
	if w <= 0 || n > math.MaxInt64 {
		r.bad, r.b = true, nil
		return 0
	}
	r.b = r.b[w:]
	return int64(n)
}

func (r *tokenReader) varint() int64 {
	n, w := binary.Varint(r.b)
	if w <= 0 {
		r.bad, r.b = true, nil
		return 0
	}
	r.b = r.b[w:]
	return n
}

// waitToken decodes tok, nil for "", and refuses what Wait refuses before any read.
func (e *Engine) waitToken(cat *Catalog, tok string) (*token, error) {
	t, err := decodeToken(tok)
	switch {
	case err != nil || t == nil:
		return nil, err
	case t.region != e.region:
		return nil, fmt.Errorf("the token is of region %s, not %s: wait in the region of the append, whose projector writes its receipts", t.region, e.region)
	case len(t.events) > MaxWaitEvents:
		return nil, fmt.Errorf("the token names %d events, over the %d that a wait reads", len(t.events), MaxWaitEvents)
	case cat.streams[t.stream] == nil:
		return nil, fmt.Errorf("the token names stream %s, which the catalog doesn't have", t.stream)
	}
	for _, ev := range t.events {
		switch {
		case ev.flags&flagReceipt == 0:
			return nil, fmt.Errorf("event %s was appended without a receipt: append with dynaflux.Waitable() to wait for it", keys.EventIDText(ev.id))
		case ev.flags&flagForeign != 0:
			return nil, fmt.Errorf("event %s was stored by another region, whose projector writes its receipt", keys.EventIDText(ev.id))
		}
	}
	return t, nil
}

// A wait polls at once, then after each of pollWaits, then every pollEvery, each ±20 %.
var pollWaits = [...]time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} //nolint:gochecknoglobals // read-only, as a constant

const pollEvery = 500 * time.Millisecond

// Wait polls, with consistent reads, the receipts of the events that tok names until every
// one is stored, timeout passes (ErrWaitTimeout) or ctx ends (its error). Before any read it
// refuses a token of another region, of more than MaxWaitEvents events or of a stream that cat
// doesn't have, and one with an event appended without a receipt or stored by another region.
// A zero token waits for nothing.
func (e *Engine) Wait(ctx context.Context, cat *Catalog, tok string, timeout time.Duration) error {
	t, err := e.waitToken(cat, tok)
	if err != nil || t == nil {
		return err
	}
	m := meta.New(e.b, e.naming(cat).MetaTable())
	deadline := time.Now().Add(timeout)
	missing := make([]meta.ReceiptKey, len(t.events))
	for i, ev := range t.events {
		missing[i] = ev.receipt()
	}
	for n := 0; ; n++ {
		stored, rerr := m.Receipts(ctx, t.stream, missing)
		if rerr != nil {
			return rerr
		}
		var still []meta.ReceiptKey
		for i, k := range missing {
			if !stored[i] {
				still = append(still, k)
			}
		}
		missing = still
		left := time.Until(deadline)
		switch {
		case len(missing) == 0:
			return nil
		case left <= 0:
			return fmt.Errorf("%w: %d of %d events of %s have no receipt after %v", ErrWaitTimeout, len(missing), len(t.events), t.stream, timeout)
		}
		if err = sleep(ctx, min(pollWait(tok, n), left)); err != nil {
			return err
		}
	}
}

// pollWait returns the wait after poll n of tok's receipts. The ±20 % hashes the token and n,
// which keeps a simulation run replayable.
func pollWait(tok string, n int) time.Duration {
	d := pollEvery
	if n < len(pollWaits) {
		d = pollWaits[n]
	}
	spread := int(2 * d / 5 / time.Microsecond)
	return d*4/5 + time.Duration(keys.Bucket(tok+"\x00"+strconv.Itoa(n), spread+1))*time.Microsecond
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// waitFor is a query's wait. A query over a stream polls nothing: its strongly consistent
// read in the append's region sees the events.
func (e *Engine) waitFor(ctx context.Context, cat *Catalog, q *query, w Wait) error {
	if q.view == nil {
		_, err := e.waitToken(cat, w.Token)
		return err
	}
	return e.Wait(ctx, cat, w.Token, w.Timeout)
}
