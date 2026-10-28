// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/meta"
)

// stageErrors stages the records of the stream's error prefix, which a job reads first
// (ADR 0016 §7), an object at a time after the last one staged. A run may hold part of an
// object, which is read again from its start: the merge drops the repeats. A read that fails
// commits what was read before it. An object written before the stream table was created
// holds an older table's records only.
func (s *step) stageErrors(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	dec, err := s.decoder(j.Stream)
	if err != nil {
		return 0, false, err
	}
	born, err := s.bornAt(ctx, j.Stream)
	if err != nil {
		return 0, false, err
	}
	objects, err := s.env.Archive.ErrorObjects(ctx, j.Stream, j.Archive.Errors.After, born)
	if err != nil {
		return 0, false, fmt.Errorf("backfill: list the error prefix of %s: %w", j.Stream, err)
	}
	to := j.Archive
	if len(objects) == 0 {
		to.Errors.Done = true
		return 0, false, s.commitArchive(ctx, &j, nil, to, false)
	}
	var buf []record
	size := 0
	for _, object := range objects {
		if !s.left() {
			break
		}
		foreign, rerr := s.readErrors(ctx, j.Stream, object, dec, func(r record) error {
			if r.older(born) {
				return nil
			}
			buf, size = append(buf, r), size+r.size
			if size < s.budget.buffer {
				return nil
			}
			if cerr := s.commitArchive(ctx, &j, buf, to, false); cerr != nil {
				return cerr
			}
			buf, size = nil, 0
			return nil
		})
		if errors.Is(rerr, ErrIncomplete) {
			return 0, false, rerr
		}
		if rerr != nil {
			return 0, false, errors.Join(rerr, s.commitArchive(ctx, &j, buf, to, false))
		}
		to.Errors.After, to.Errors.Foreign = object, to.Errors.Foreign+foreign
	}
	return 0, false, s.commitArchive(ctx, &j, buf, to, false)
}

// readErrors adds the records of an error object that Dynaflux wrote, and returns how many
// lines it skipped because Dynaflux didn't write them. An object with anything else, such as
// the data files of a commit that failed, makes the archive incomplete: it records an errors
// hole and fails with ErrIncomplete.
func (s *step) readErrors(ctx context.Context, stream, object string, dec *archive.Decoder, add func(record) error) (int64, error) {
	var foreign int64
	var bad string
	_, err := s.readLines(ctx, object, func(_ int, line []byte) (bool, error) {
		if len(line) == 0 {
			return true, nil
		}
		rec, why := errorRecord(dec, line)
		switch {
		case why != "":
			bad = why
			return false, nil
		case rec == nil:
			foreign++
			return true, nil
		}
		return true, add(*rec)
	})
	if err != nil || bad == "" {
		return foreign, err
	}
	if err = s.env.Meta.MarkErrors(ctx, stream, object, time.Now()); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("%w: %s in %s: the error prefix holds %s: %s", ErrIncomplete, stream, s.env.Region, object, bad)
}

// errorRecord reads a line of an error object: the record that its rawData holds when that
// has the columns _shard and _seq; nil when it lacks them, as a record Dynaflux didn't write
// does; or why the line leaves the archive incomplete.
func errorRecord(dec *archive.Decoder, line []byte) (*record, string) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(line, &envelope); err != nil {
		return nil, "a line isn't JSON"
	}
	raw, ok := envelope["rawData"]
	if !ok {
		return nil, "a line holds no record: the data files of a commit that failed, or something else"
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return nil, "a line's rawData isn't a string"
	}
	data, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return nil, "a line's rawData isn't base64"
	}
	var cols map[string]json.RawMessage
	if json.Unmarshal(data, &cols) != nil {
		return nil, ""
	}
	if _, ok = cols[archive.ColumnShard]; !ok {
		return nil, ""
	}
	if _, ok = cols[archive.ColumnSeq]; !ok {
		return nil, ""
	}
	r, err := dec.Decode(data)
	if err != nil {
		return nil, "a record of Dynaflux doesn't decode: " + err.Error()
	}
	rec := archived(r, len(data))
	return &rec, ""
}
