// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package backfill

import (
	"cmp"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"slices"

	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
)

// budget is how a step spends Env.Memory: the encoded bytes of records it sorts before it
// writes a run, the encoded bytes of a block, and how many chains of runs a merge reads at
// once.
type budget struct {
	buffer, block int
	fanIn         int
}

const (
	maxBuffer = 512 << 20
	maxBlock  = 256 << 10
	maxFanIn  = 128
	minFanIn  = 2
	// A decoded record takes up to decodedFactor times its encoded bytes in the heap.
	decodedFactor = 10
	// The sort buffer's decoded records take a bufferShare of the memory, and a block a
	// blockShare of the buffer.
	bufferShare = 4
	blockShare  = 4
	// A merge holds a decoded block per input and writes as much, so each input costs
	// mergeShare blocks.
	mergeShare = 2 * decodedFactor
)

func budgetOf(memory int64) budget {
	buffer := int(min(memory/(bufferShare*decodedFactor), maxBuffer))
	block := max(1, min(buffer/blockShare, maxBlock))
	return budget{buffer: buffer, block: block, fanIn: int(min(max(memory/int64(mergeShare*block), minFanIn), maxFanIn))}
}

// chains groups runs, in their order, into chains whose runs follow one another in mark
// order, as a merge pass's outputs do. A merge reads a chain's runs one after the other, so it
// holds one block per chain, and each pass leaves fewer chains however many steps it spans.
func chains(runs []meta.Run) [][]meta.Run {
	var out [][]meta.Run
	for _, r := range runs {
		if n := len(out); n > 0 && r.First.Compare(out[n-1][len(out[n-1])-1].Last) > 0 {
			out[n-1] = append(out[n-1], r)
			continue
		}
		out = append(out, []meta.Run{r})
	}
	return out
}

// chainReader reads a chain's records in order, from the first after a mark.
type chainReader struct {
	runs   Runs
	prefix string
	chain  []meta.Run // the runs not opened yet
	after  *keys.Mark
	cur    *runReader
}

// more reports whether a record is left, loading it. A run that ends at or before the mark
// isn't read.
func (c *chainReader) more(ctx context.Context) (bool, error) {
	for {
		if c.cur != nil {
			if _, ok, err := c.cur.peek(ctx); err != nil || ok {
				return ok, err
			}
			c.cur = nil
		}
		for len(c.chain) > 0 && c.after != nil && c.chain[0].Last.Compare(*c.after) <= 0 {
			c.chain = c.chain[1:]
		}
		if len(c.chain) == 0 {
			return false, nil
		}
		r, err := openRun(ctx, c.runs, c.prefix+c.chain[0].Name, c.after)
		if err != nil {
			return false, err
		}
		c.cur, c.chain = r, c.chain[1:]
	}
}

// merger yields the records of chains in order, each repeat once: a record staged twice.
type merger struct {
	heads   []*chainReader // a heap, by each chain's next record
	prev    *record
	repeats int64
}

func openMerge(ctx context.Context, runs Runs, prefix string, cs [][]meta.Run, after *keys.Mark) (*merger, error) {
	m := &merger{}
	for _, chain := range cs {
		c := &chainReader{runs: runs, prefix: prefix, chain: chain, after: after}
		ok, err := c.more(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			m.heads = append(m.heads, c)
		}
	}
	heap.Init(m)
	return m, nil
}

// next returns the next record, or false after the last.
func (m *merger) next(ctx context.Context) (record, bool, error) {
	for len(m.heads) > 0 {
		c := m.heads[0]
		rec := c.cur.recs[0]
		c.cur.recs = c.cur.recs[1:]
		more, err := c.more(ctx)
		if err != nil {
			return record{}, false, err
		}
		if more {
			heap.Fix(m, 0)
		} else {
			heap.Pop(m)
		}
		if m.prev != nil && repeats(m.prev, &rec) {
			m.repeats++
			continue
		}
		m.prev = &rec
		return rec, true, nil
	}
	return record{}, false, nil
}

func (m *merger) Len() int { return len(m.heads) }
func (m *merger) Less(i, j int) bool {
	return compareRecords(&m.heads[i].cur.recs[0], &m.heads[j].cur.recs[0]) < 0
}
func (m *merger) Swap(i, j int) { m.heads[i], m.heads[j] = m.heads[j], m.heads[i] }

// Push is never called: heads only leave the heap.
func (m *merger) Push(any) {}
func (m *merger) Pop() any {
	last := m.heads[len(m.heads)-1]
	m.heads = m.heads[:len(m.heads)-1]
	return last
}

// putRun writes the records that add gives a run under a new name of the job's, and returns
// its entry, or false when add gave none.
func (s *step) putRun(ctx context.Context, j *meta.Job, add func(w *runWriter) error) (meta.Run, bool, error) {
	name, err := s.runName(j)
	if err != nil {
		return meta.Run{}, false, err
	}
	var run meta.Run
	var ok bool
	err = s.env.Runs.Put(ctx, j.Staging+name, func(out io.Writer) error {
		w := &runWriter{w: out, block: s.budget.block}
		if aerr := add(w); aerr != nil {
			return aerr
		}
		var cerr error
		run, ok, cerr = w.close(name)
		return cerr
	})
	if err != nil {
		return meta.Run{}, false, fmt.Errorf("backfill: write run %s: %w", name, err)
	}
	return run, ok, nil
}

// sortedRun writes buf, sorted and without repeats, as a run of the job's, and returns its
// entry, or false when buf is empty.
func (s *step) sortedRun(ctx context.Context, j *meta.Job, buf []record) (meta.Run, bool, error) {
	if len(buf) == 0 {
		return meta.Run{}, false, nil
	}
	slices.SortFunc(buf, func(a, b record) int { return compareRecords(&a, &b) })
	buf = slices.CompactFunc(buf, func(a, b record) bool { return repeats(&a, &b) })
	return s.putRun(ctx, j, func(w *runWriter) error {
		for i := range buf {
			if err := w.add(&buf[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// runName returns a name no other writer uses: a stale or duplicate worker, whose runs have
// other extents, can then add garbage but never replace a committed run.
func (s *step) runName(j *meta.Job) (string, error) {
	nonce, err := s.nonce()
	return fmt.Sprintf("runs/%d-%s", j.Attempt, nonce), err
}

// nonce returns 16 hex digits from the step's entropy, which names a staged object.
func (s *step) nonce() (string, error) {
	var b [8]byte
	entropy := s.env.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	if _, err := io.ReadFull(entropy, b[:]); err != nil {
		return "", fmt.Errorf("backfill: name a staged object: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// mergePass merges the job's smallest chains, a fan-in of them, so that no record is merged
// much more often than others, or resumes the pass in progress. It commits each run it writes
// with the last mark it holds; the pass's last commit replaces its inputs with its outputs,
// one chain. A pass that another worker moved on ends without error, for the step to read
// the job again.
func (s *step) mergePass(ctx context.Context, j meta.Job, cs [][]meta.Run) error {
	inputs, after := names(slices.Concat(smallest(cs, s.budget.fanIn)...)), (*keys.Mark)(nil)
	if j.Merge != nil {
		inputs, after = j.Merge.Inputs, j.Merge.Mark
	}
	// The inputs in the job's order, so that every worker groups them in the same chains.
	in := slices.DeleteFunc(slices.Clone(j.Runs), func(r meta.Run) bool { return !slices.Contains(inputs, r.Name) })
	if len(in) != len(inputs) {
		return fmt.Errorf("backfill: %s: a merge pass's input isn't a run of the job", s.version)
	}
	inputs = names(in)
	m, err := openMerge(ctx, s.env.Runs, j.Staging, chains(in), after)
	if err != nil {
		return err
	}
	for {
		run, wrote, perr := s.putRun(ctx, &j, func(w *runWriter) error { return s.mergeOutput(ctx, m, w) })
		if perr != nil {
			return perr
		}
		end := m.Len() == 0
		if perr = s.commitMerge(ctx, inputs, after, run, wrote, end); perr != nil || end || !s.left() {
			return perr
		}
		after = &run.Last
	}
}

// smallest returns the n chains of cs with the fewest records, the earlier of two equal ones
// first.
func smallest(cs [][]meta.Run, n int) [][]meta.Run {
	records := func(c []meta.Run) int64 {
		var sum int64
		for _, r := range c {
			sum += r.Records
		}
		return sum
	}
	sorted := slices.Clone(cs)
	slices.SortStableFunc(sorted, func(a, b []meta.Run) int { return cmp.Compare(records(a), records(b)) })
	return sorted[:n]
}

// mergeOutput writes m's records to w until the step's cutoff or a buffer's worth, at least
// one: a pass commits its progress that often.
func (s *step) mergeOutput(ctx context.Context, m *merger, w *runWriter) error {
	for w.last == nil || s.left() && w.size() < s.budget.buffer {
		rec, ok, err := m.next(ctx)
		if err != nil || !ok {
			return err
		}
		if err = w.add(&rec); err != nil {
			return err
		}
	}
	return nil
}

// commitMerge records a pass's output in the job: run, unless wrote is false, as the output
// after the one whose last mark is after. With end it replaces the pass's inputs with its
// outputs.
func (s *step) commitMerge(ctx context.Context, inputs []string, after *keys.Mark, run meta.Run, wrote, end bool) error {
	_, err := s.update(ctx, meta.PhaseApply, func(j *meta.Job) (bool, error) {
		mg := j.Merge
		switch {
		case mg != nil && wrote && slices.ContainsFunc(mg.Outputs, func(o meta.Run) bool { return o.Name == run.Name }):
			// The SDK retried this write after a lost response.
			return false, nil
		case mg == nil && after != nil, mg != nil && (!slices.Equal(mg.Inputs, inputs) || !sameMark(mg.Mark, after)):
			return false, errMoved
		case mg == nil:
			for _, in := range inputs {
				if !slices.ContainsFunc(j.Runs, func(r meta.Run) bool { return r.Name == in }) {
					return false, errMoved
				}
			}
			mg = &meta.Merge{Inputs: inputs}
		}
		next := *mg
		if wrote {
			next.Outputs = append(slices.Clone(mg.Outputs), run)
			next.Mark = &run.Last
		}
		j.Merge = &next
		if end {
			rest := slices.DeleteFunc(j.Runs, func(r meta.Run) bool { return slices.Contains(inputs, r.Name) })
			j.Runs, j.Merge = slices.Concat(next.Outputs, rest), nil
		}
		return true, nil
	})
	return err
}

func sameMark(a, b *keys.Mark) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func names(runs []meta.Run) []string {
	out := make([]string, len(runs))
	for i, r := range runs {
		out[i] = r.Name
	}
	return out
}
