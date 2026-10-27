// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package firehose writes a projector batch's INSERT records to the stream's Firehose
// stream, which feeds the region's Iceberg table. A batch succeeds only once Firehose has
// accepted every record, so the projector's progress never passes a record that the
// archive lacks.
package firehose

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/projector"
)

// PutRecordBatch takes at most 500 records and "4 MB for the entire request". Whether
// that counts raw or base64 bytes isn't documented, so a call's JSON request stays within
// 4,000,000 bytes: the records' base64, plus recordOverhead each for {"Data":""} and a
// comma, plus requestOverhead and the stream name for the rest.
const (
	maxCallRecords  = 500
	maxRequestBytes = 4_000_000
	recordOverhead  = 12
	requestOverhead = 64
)

// Records that Firehose didn't accept are resent after a random wait of up to a delay
// that starts at firstDelay and grows by delayGrowth up to maxDelay, until the time left
// when Archive started, divided by resendShare, has passed. The rest is for rows and
// progress.
const (
	firstDelay  = 100 * time.Millisecond
	maxDelay    = 5 * time.Second
	delayGrowth = 2
	resendShare = 2
)

// Sender makes one PutRecordBatch call. codes is aligned with recs: "" for a record that
// got a RecordId, its ErrorCode otherwise.
type Sender interface {
	PutBatch(ctx context.Context, stream string, recs [][]byte) (codes []string, err error)
}

// Writer archives the records of the streams that a region archives. It is safe for
// concurrent use.
type Writer struct {
	region  string
	sender  Sender
	streams map[string]*target
}

// target is an archived stream.
type target struct {
	name            string // the Firehose stream's
	dec             *projector.Stream
	enc             *archive.Encoder
	eventID, origin int  // the indexes of event_id and origin_region in dec's values
	local           bool // archive.events is local
}

// NewWriter returns the archiver of region, which sends through s: one Firehose stream per
// stream of c that region archives, named by naming.
func NewWriter(c *projector.Compiled, naming catalog.Naming, region string, s Sender) *Writer {
	w := &Writer{region: region, sender: s, streams: map[string]*target{}}
	for dec := range c.Streams() {
		st := dec.Def
		if !archive.Archived(st, region) {
			continue
		}
		w.streams[st.Name] = &target{
			name:    naming.ArchiveStream(st.Name),
			dec:     dec,
			enc:     archive.NewEncoder(st),
			eventID: dec.Index(catalog.ColumnEventID),
			origin:  dec.Index(catalog.ColumnOriginRegion),
			local:   st.Archive.Events == catalog.ArchiveEventsLocal,
		}
	}
	return w
}

// Archive implements [projector.Archiver]. It returns nil only when Firehose gave a
// RecordId to every record that this region archives: none for a stream it doesn't
// archive, and with archive.events local, only the records that originated here. It
// resends the records Firehose didn't accept until half the time left to ctx's deadline
// has passed. A record it can never send fails the batch before any call: one over
// Firehose's record limit, or one whose values don't decode.
func (w *Writer) Archive(ctx context.Context, stream, unit string, recs []projector.Record) error {
	t, ok := w.streams[stream]
	if !ok {
		return nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("firehose: archiving needs a context with a deadline")
	}
	stop := time.Now().Add(time.Until(deadline) / resendShare)
	data, err := w.encode(t, unit, recs)
	if err != nil || len(data) == 0 {
		return err
	}
	return w.send(ctx, t.name, unit, data, stop)
}

// send puts recs to stream, and resends the records Firehose didn't accept until stop.
func (w *Writer) send(ctx context.Context, stream, unit string, recs [][]byte, stop time.Time) error {
	for delay := firstDelay; ; delay = min(delayGrowth*delay, maxDelay) {
		failed, code, err := w.put(ctx, stream, recs)
		if err != nil || len(failed) == 0 {
			return err
		}
		wait := rand.N(delay) //nolint:gosec // jitter, not a secret
		if time.Now().Add(wait).After(stop) {
			return fmt.Errorf("firehose: %s didn't accept %d records of unit %s in time: %s", stream, len(failed), unit, code)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("firehose: %w", ctx.Err())
		case <-timer.C:
		}
		recs = failed
	}
}

// encode returns the archive records of recs that this region archives.
func (w *Writer) encode(t *target, unit string, recs []projector.Record) ([][]byte, error) {
	var out [][]byte
	for _, r := range recs {
		if r.Op != projector.Insert {
			return nil, fmt.Errorf("firehose: unit %s, seq %s: only INSERT records are archived", unit, r.Seq)
		}
		vals, bad := t.dec.Decode(r.New)
		// Another region's event is that region's to archive. An origin that doesn't decode
		// fails below, as it does with archive.events all.
		if t.local && (bad == nil || bad[t.origin] == nil) && vals[t.origin].AsString() != w.region {
			continue
		}
		seq, err := keys.PadSeq(r.Seq)
		if err != nil {
			return nil, fmt.Errorf("firehose: unit %s: %w", unit, err)
		}
		var rec []byte
		if err = errors.Join(bad...); err == nil {
			rec = t.enc.AppendRecord(nil, vals, unit, seq, r.Arrival)
			if len(rec) > archive.MaxRecordSize {
				err = fmt.Errorf("the record is %d bytes, over Firehose's record limit of %d", len(rec), archive.MaxRecordSize)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("firehose: can't archive unit %s, seq %s, event_id %s: %w", unit, r.Seq, vals[t.eventID].AsString(), err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// put sends recs in as few calls as the limits allow, and returns the records Firehose
// didn't accept with the error code of the last one. A call that fails, which the SDK has
// already retried, fails at once.
func (w *Writer) put(ctx context.Context, stream string, recs [][]byte) ([][]byte, string, error) {
	var failed [][]byte
	code := ""
	for len(recs) > 0 {
		n := callLen(stream, recs)
		codes, err := w.sender.PutBatch(ctx, stream, recs[:n])
		if err != nil {
			return nil, "", fmt.Errorf("firehose: put %d records to %s: %w", n, stream, err)
		}
		if len(codes) != n {
			// Not the documented response, so no record counts as accepted.
			failed = append(failed, recs[:n]...)
			code = fmt.Sprintf("%d responses to %d records", len(codes), n)
			codes = nil
		}
		for i, c := range codes {
			if c != "" {
				failed, code = append(failed, recs[i]), c
			}
		}
		recs = recs[n:]
	}
	return failed, code, nil
}

// callLen returns how many leading records of recs fit in one call to stream: at least one,
// since encode refuses a record over archive.MaxRecordSize.
func callLen(stream string, recs [][]byte) int {
	size := requestOverhead + len(stream)
	for i, r := range recs {
		size += base64.StdEncoding.EncodedLen(len(r)) + recordOverhead
		if i == maxCallRecords || size > maxRequestBytes {
			return i
		}
	}
	return len(recs)
}
