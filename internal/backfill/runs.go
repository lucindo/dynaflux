// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// A run is a sequence of independently gzipped blocks, each a JSON line per record, then a
// JSON index of the blocks and a fixed-size trailer: the index's offset and length, and
// runMagic. A reader seeks to the block that holds a mark without reading the ones before.
const (
	runMagic   = "dfrun001"
	trailerLen = 8 + 4 + len(runMagic)
	// maxStored bounds an index or a block as stored, and a block decompressed: a writer
	// makes blocks of at most a budget's block bytes plus one record.
	maxStored = 64 << 20
	maxOffset = 1 << 62
	tailRead  = 64 << 10
)

// record is a staged record: its B mark, and for an archive record its delivery unit and
// sequence number as delivered; a table item has neither, and its mark is its time mark.
// size is its encoded length, for the memory budget.
type record struct {
	mark      keys.Mark
	unit, seq string
	item      store.Item
	size      int
}

// backfillRecord returns r as ApplyBackfill takes it.
func (r *record) backfillRecord() projector.BackfillRecord {
	if r.unit == "" {
		return projector.BackfillRecord{Item: r.item, Mark: r.mark}
	}
	return projector.BackfillRecord{Unit: r.unit, Seq: r.seq, Item: r.item}
}

// compareRecords orders records as the backfill applies them: by mark, then by unit, so that
// repeats are adjacent.
func compareRecords(a, b *record) int {
	return cmp.Or(a.mark.Compare(b.mark), strings.Compare(a.unit, b.unit))
}

// repeats reports whether b repeats a: the same record, staged twice.
func repeats(a, b *record) bool { return a.mark == b.mark && a.unit == b.unit }

type runIndex struct {
	Records int64      `json:"records"`
	Blocks  []blockRef `json:"blocks"`
}

type blockRef struct {
	Offset int64    `json:"o"`
	Size   int64    `json:"n"`
	First  markJSON `json:"f"`
}

type markJSON struct {
	UnitKey string `json:"k"`
	Seq     string `json:"q"`
}

type recordJSON struct {
	Mark markJSON                   `json:"m"`
	Unit string                     `json:"u,omitempty"`
	Seq  string                     `json:"s,omitempty"`
	Item map[string]store.JSONValue `json:"i"`
}

// runWriter writes a run of records added in order.
type runWriter struct {
	w     io.Writer
	block int // the block size, in encoded bytes
	off   int64
	buf   bytes.Buffer // the current block's lines
	first keys.Mark    // of the current block
	index runIndex
	last  *record
}

func (w *runWriter) add(r *record) error {
	if w.last != nil && compareRecords(w.last, r) > 0 {
		return fmt.Errorf("backfill: a run's records out of order at mark %v", r.mark)
	}
	line, err := json.Marshal(recordJSON{
		Mark: markJSON{UnitKey: r.mark.UnitKey, Seq: r.mark.Seq},
		Unit: r.unit,
		Seq:  r.seq,
		Item: store.EncodeJSON(r.item),
	})
	if err != nil {
		return fmt.Errorf("backfill: encode a record: %w", err)
	}
	if w.buf.Len() == 0 {
		w.first = r.mark
	}
	w.buf.Write(line)
	w.buf.WriteByte('\n')
	w.index.Records++
	w.last = r
	if w.buf.Len() >= w.block {
		return w.flush()
	}
	return nil
}

// size is the bytes the run's records take so far, encoded.
func (w *runWriter) size() int { return int(w.off) + w.buf.Len() }

func (w *runWriter) flush() error {
	if w.buf.Len() == 0 {
		return nil
	}
	var z bytes.Buffer
	zw := gzip.NewWriter(&z)
	if _, err := zw.Write(w.buf.Bytes()); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	n, err := w.w.Write(z.Bytes())
	if err != nil {
		return err
	}
	w.index.Blocks = append(w.index.Blocks, blockRef{Offset: w.off, Size: int64(n), First: markJSON{w.first.UnitKey, w.first.Seq}})
	w.off += int64(n)
	w.buf.Reset()
	return nil
}

// close writes the last block, the index and the trailer, and returns the run's entry for
// the job item, or false when it holds no record.
func (w *runWriter) close(name string) (meta.Run, bool, error) {
	if err := w.flush(); err != nil {
		return meta.Run{}, false, err
	}
	idx, err := json.Marshal(w.index)
	if err != nil {
		return meta.Run{}, false, err
	}
	trailer := binary.BigEndian.AppendUint64(nil, uint64(w.off))       //nolint:gosec // an offset is never negative
	trailer = binary.BigEndian.AppendUint32(trailer, uint32(len(idx))) //nolint:gosec // an index is far below 4 GiB
	if _, err = w.w.Write(append(append(idx, trailer...), runMagic...)); err != nil {
		return meta.Run{}, false, err
	}
	if w.last == nil {
		return meta.Run{}, false, nil
	}
	first := w.index.Blocks[0].First
	return meta.Run{Name: name, Records: w.index.Records, First: keys.Mark{UnitKey: first.UnitKey, Seq: first.Seq}, Last: w.last.mark}, true, nil
}

// runReader reads a run's records in order, from the first after a mark.
type runReader struct {
	runs  Runs
	name  string
	index runIndex
	next  int      // the next block to load
	recs  []record // what is left of the loaded block
}

// openRun opens a run at the first record after after, or at its first record when after is
// nil. It reads the run's last tailRead bytes, which hold the index of most runs too.
func openRun(ctx context.Context, runs Runs, name string, after *keys.Mark) (*runReader, error) {
	tail, err := runs.ReadAt(ctx, name, -tailRead, tailRead)
	if err != nil {
		return nil, fmt.Errorf("backfill: run %s: %w", name, err)
	}
	if len(tail) < trailerLen || string(tail[len(tail)-len(runMagic):]) != runMagic {
		return nil, fmt.Errorf("backfill: run %s has no trailer", name)
	}
	trailer := tail[len(tail)-trailerLen:]
	off, n := binary.BigEndian.Uint64(trailer), binary.BigEndian.Uint32(trailer[8:])
	if off > maxOffset || n > maxStored {
		return nil, fmt.Errorf("backfill: run %s: an index of %d bytes at %d", name, n, off)
	}
	data := tail[:len(tail)-trailerLen]
	if int(n) <= len(data) {
		data = data[len(data)-int(n):]
	} else if data, err = runs.ReadAt(ctx, name, int64(off), int64(n)); err != nil {
		return nil, fmt.Errorf("backfill: run %s: %w", name, err)
	}
	r := &runReader{runs: runs, name: name}
	if err = json.Unmarshal(data, &r.index); err != nil {
		return nil, fmt.Errorf("backfill: run %s: index: %w", name, err)
	}
	if after != nil {
		// The first block whose next block starts past after.
		first := sort.Search(len(r.index.Blocks), func(i int) bool {
			f := r.index.Blocks[i].First
			return keys.Mark{UnitKey: f.UnitKey, Seq: f.Seq}.Compare(*after) > 0
		})
		r.next = max(0, first-1)
	}
	for {
		rec, ok, perr := r.peek(ctx)
		if perr != nil || !ok || after == nil || rec.mark.Compare(*after) > 0 {
			return r, perr
		}
		r.recs = r.recs[1:]
	}
}

// peek returns the next record without taking it, or false at the run's end.
func (r *runReader) peek(ctx context.Context) (*record, bool, error) {
	for len(r.recs) == 0 {
		if r.next >= len(r.index.Blocks) {
			return nil, false, nil
		}
		if err := r.load(ctx, r.index.Blocks[r.next]); err != nil {
			return nil, false, fmt.Errorf("backfill: run %s block %d: %w", r.name, r.next, err)
		}
		r.next++
	}
	return &r.recs[0], true, nil
}

func (r *runReader) load(ctx context.Context, b blockRef) error {
	if b.Offset < 0 || b.Size < 0 || b.Size > maxStored {
		return fmt.Errorf("%d bytes at %d", b.Size, b.Offset)
	}
	data, err := r.runs.ReadAt(ctx, r.name, b.Offset, b.Size)
	if err != nil {
		return err
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(zr, maxStored+1))
	if err != nil {
		return err
	}
	if len(raw) > maxStored {
		return errors.New("a block over its size limit")
	}
	for line := range bytes.Lines(raw) {
		var rj recordJSON
		if err = json.Unmarshal(line, &rj); err != nil {
			return err
		}
		it, derr := store.DecodeJSON(rj.Item)
		if derr != nil {
			return derr
		}
		r.recs = append(r.recs, record{mark: keys.Mark{UnitKey: rj.Mark.UnitKey, Seq: rj.Mark.Seq}, unit: rj.Unit, seq: rj.Seq, item: it, size: len(line)})
	}
	return nil
}
