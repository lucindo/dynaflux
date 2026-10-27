// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"bufio"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
)

// maxLine bounds a line of an export file: an archive record, with room for JSON escapes.
const maxLine = 4 * archive.MaxRecordSize

// stage reads the archive's export into runs, each committed to the job with the records it
// holds. A run's name ends with how many of the export's files the committed runs hold in
// full, and a step resumes at the next one; the records of a file that a run holds in part are
// read again, and the merge drops repeats. The export's files must be the same at every step.
// That count rests on the runs the step started from and the ones it committed since, so a
// commit needs them all still listed: after a restage cleared them, a stale step's run would
// claim files no run holds.
func (s *step) stage(ctx context.Context, j meta.Job) (time.Duration, bool, error) {
	files, done, err := s.env.Archive.Export(ctx, &j)
	switch {
	case err != nil:
		return 0, false, fmt.Errorf("backfill: export the archive of %s: %w", j.Stream, err)
	case !done:
		return exportPoll, false, nil
	}
	def, ok := s.env.Catalog.Stream(j.Stream)
	if !ok {
		return 0, false, fmt.Errorf("backfill: no stream %s in this build's catalog", j.Stream)
	}
	dec := archive.NewDecoder(def)
	var buf []record
	size, held := 0, names(j.Runs)
	for i := stagedFiles(j.Runs); i < len(files); i++ {
		if !s.left() {
			return 0, false, s.commitStaged(ctx, &j, buf, i, false, &held)
		}
		err = s.readFile(ctx, files[i], dec, func(r record) error {
			buf, size = append(buf, r), size+r.size
			if size < s.budget.buffer {
				return nil
			}
			cerr := s.commitStaged(ctx, &j, buf, i, false, &held)
			buf, size = nil, 0
			return cerr
		})
		if err != nil {
			return 0, false, err
		}
	}
	return 0, false, s.commitStaged(ctx, &j, buf, len(files), true, &held)
}

// stagedFiles returns how many export files the committed runs hold in full: the most that a
// run's name ends with.
func stagedFiles(runs []meta.Run) int {
	n := 0
	for _, r := range runs {
		if i := strings.LastIndex(r.Name, fileSuffix); i >= 0 {
			if k, err := strconv.Atoi(r.Name[i+len(fileSuffix):]); err == nil {
				n = max(n, k)
			}
		}
	}
	return n
}

// fileSuffix precedes, in a staged run's name, how many export files the committed runs hold
// in full.
const fileSuffix = "-f"

func (s *step) readFile(ctx context.Context, file string, dec *archive.Decoder, add func(record) error) error {
	rc, err := s.env.Archive.Open(ctx, file)
	if err != nil {
		return fmt.Errorf("backfill: open %s: %w", file, err)
	}
	sc := bufio.NewScanner(rc)
	sc.Buffer(nil, maxLine)
	for err == nil && sc.Scan() {
		unit, seq, it, derr := dec.Decode(sc.Bytes())
		if derr != nil {
			err = fmt.Errorf("backfill: %s: %w", file, derr)
			break
		}
		err = add(record{mark: keys.Mark{UnitKey: keys.UnitKey(unit), Seq: seq}, unit: unit, seq: seq, item: it, size: len(sc.Bytes())})
	}
	if err == nil {
		err = sc.Err()
	}
	if cerr := rc.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("backfill: close %s: %w", file, cerr)
	}
	return err
}

// commitStaged writes buf as a run, after which the committed runs hold files export files in
// full, and adds it to the job, moving the job to apply when last. held are the runs the claim
// rests on, the run's name joining them.
func (s *step) commitStaged(ctx context.Context, j *meta.Job, buf []record, files int, last bool, held *[]string) error {
	run, wrote, err := s.sortedRun(ctx, j, fileSuffix+strconv.Itoa(files), buf)
	if err != nil {
		return err
	}
	*j, err = s.update(ctx, meta.PhaseStage, func(j *meta.Job) (bool, error) {
		listed := func(name string) bool {
			return slices.ContainsFunc(j.Runs, func(r meta.Run) bool { return r.Name == name })
		}
		if slices.ContainsFunc(*held, func(name string) bool { return !listed(name) }) {
			return false, errMoved
		}
		add := wrote && !listed(run.Name)
		if add {
			j.Runs = append(j.Runs, run)
			j.Staged += run.Records
		}
		if last {
			j.Phase = meta.PhaseApply
		}
		return add || last, nil
	})
	if err == nil && wrote {
		*held = append(*held, run.Name)
	}
	return err
}
