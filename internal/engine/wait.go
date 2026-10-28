// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
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

// Token returns the text of a token that names stored, the events that an append to stream
// stored or found stored, or "" when there are none: the engine's region and the stream,
// then each event's sort key, its ingest_time as stored, and its flags. It leaves out the
// partition key, which holds the event's PRIMARY KEY values: a token can reach clients and
// logs.
func (e *Engine) Token(stream string, stored []store.Item) string {
	if len(stored) == 0 {
		return ""
	}
	b := appendText([]byte{tokenFormat}, e.region)
	b = appendText(b, stream)
	for _, it := range stored {
		b = appendText(b, it[catalog.AttrSK].Str)
		b = appendText(b, it[catalog.AttrIngestTime].Str)
		var flags byte
		if rcpt := it[catalog.AttrReceipt]; rcpt.Type == store.BOOL && rcpt.Bool {
			flags |= flagReceipt
		}
		if it[catalog.AttrOriginRegion].Str != e.region {
			flags |= flagForeign
		}
		b = append(b, flags)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func appendText(b []byte, s string) []byte {
	return append(binary.AppendUvarint(b, uint64(len(s))), s...)
}

// token is a token decoded.
type token struct {
	region, stream string
	events         []tokenEvent
}

type tokenEvent struct {
	receipt meta.ReceiptKey
	flags   byte
}

var errMalformed = errors.New("malformed token: not the text of a token that an append returned")

// CheckToken returns an error unless text is "" or the text of a token that [Engine.Token]
// returned. It checks the form only: a token holds no secret.
func CheckToken(text string) error {
	_, err := decodeToken(text)
	return err
}

// decodeToken decodes the text of a token, nil for "".
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
		_, id, kerr := keys.ParseStreamSK(d.text())
		it, ierr := strconv.ParseInt(d.text(), 10, 64)
		flags := d.flags()
		if kerr != nil || ierr != nil || flags&^flagsKnown != 0 {
			return nil, errMalformed
		}
		t.events = append(t.events, tokenEvent{receipt: meta.ReceiptKey{EventID: id, It: it}, flags: flags})
	}
	if d.bad || t.region == "" || t.stream == "" || len(t.events) == 0 {
		return nil, errMalformed
	}
	return t, nil
}

// tokenReader reads the fields of a token's binary form. bad is set at the first one that
// runs past the end.
type tokenReader struct {
	b   []byte
	bad bool
}

func (r *tokenReader) text() string {
	n, w := binary.Uvarint(r.b)
	if w <= 0 || n > uint64(len(r.b)-w) { //nolint:gosec // w is at most len(r.b)
		r.bad, r.b = true, nil
		return ""
	}
	end := w + int(n) //nolint:gosec // n is at most the bytes left
	s := string(r.b[w:end])
	r.b = r.b[end:]
	return s
}

func (r *tokenReader) flags() byte {
	if len(r.b) == 0 {
		r.bad = true
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
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
			return nil, fmt.Errorf("event %s was appended without a receipt: append with dynaflux.Waitable() to wait for it", ev.receipt.EventID)
		case ev.flags&flagForeign != 0:
			return nil, fmt.Errorf("event %s was stored by another region, whose projector writes its receipt", ev.receipt.EventID)
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
		missing[i] = ev.receipt
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
