// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

const (
	importWriters     = 32
	defaultImportRate = 500 // events a second
	importProgress    = 10 * time.Second
	// maxClockSkew is how far this machine's clock may be from DynamoDB's: import stamps
	// ingest_time with it, and the time split assumes writer clocks within 5 minutes.
	maxClockSkew = 60 * time.Second
	// maxLineMiB bounds an input line. The JSON of a 400 KB item, escaped at its worst, fits.
	maxLineMiB  = 4
	maxLineSize = maxLineMiB << 20
)

func importCommand() command {
	return command{
		name:    "import",
		args:    "[flags] -file <path|-> <stream>",
		summary: "load past events from JSON Lines into a stream of the deployed store",
		doc: `Import appends past events to a stream of the deployed store in one region, and
replication copies them to the other regions. The input is JSON Lines: one object per
line, with a key for each column and for event_time, in the forms of -json output.
A file whose name ends in .gz is read with gzip. -file - reads standard input, so
this imports an S3 object:

  aws s3 cp s3://bucket/key - | dynaflux import -file - <stream>

A missing Nullable column is NULL, and a missing column with a DEFAULT takes it.

Import checks every line against the region's deployed stream before it writes
anything, and prints each problem as file:line. -check stops after the check. Each
event is its own write, at most -rate a second. An imported event's ingest_time is
the time of the import, so a view that groups by ingest_time counts it then.

Import refuses to run on a machine whose clock is more than 60 seconds off
DynamoDB's: a view filled by time relies on the clocks of writers.

Run again on a stream with DEDUP BY, import reports the events already present and
those stored before with other values. A stream without DEDUP BY gets every event
again: -skip n resumes after line n, which import prints as it goes and when it stops.

Exit status: 0 when every event is stored or already present; 1 for invalid lines
(nothing is written), events stored before with other values, a clock that is off,
and an interrupted import; and 2 for usage and I/O problems and AWS access.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := importOptions{aws: addAWSFlags(fs)}
			fs.StringVar(&o.file, "file", "", "read the events from this `path`, or from standard input with -")
			fs.IntVar(&o.rate, "rate", defaultImportRate, "write at most `n` events a second")
			fs.IntVar(&o.skip, "skip", 0, "write only the events after line `n`")
			fs.BoolVar(&o.check, "check", false, "check the input and stop")
			fs.BoolVar(&o.yes, "yes", false, "import without asking")
			return func(c *cli, args []string) int { return c.importEvents(o, args) }
		},
	}
}

type importOptions struct {
	aws        *awsFlags
	file       string
	rate, skip int
	check, yes bool
}

func (c *cli) importEvents(o importOptions, args []string) int {
	switch {
	case len(args) != 1:
		return c.usageError("import", "name one stream")
	case o.file == "":
		return c.usageError("import", "name the input with -file, or -file - for standard input")
	case o.rate < 1:
		return c.usageError("import", "-rate must be 1 or more")
	case o.skip < 0:
		return c.usageError("import", "-skip must be 0 or more")
	}
	d, status := c.openDeployment("import", o.aws)
	if status != exitOK {
		return status
	}
	def := d.compiled.Catalog()
	s, ok := def.Stream(args[0])
	if !ok {
		names := make([]string, len(def.Streams))
		for i, st := range def.Streams {
			names[i] = st.Name
		}
		return c.usageError("import", notDeployed("stream", args[0], d.region, names))
	}
	in, err := c.openInput(o.file)
	if err != nil {
		c.errorf("dynaflux import: %v", err)
		return exitUsage
	}
	defer func() {
		if cerr := in.close(); cerr != nil {
			c.errorf("dynaflux import: %v", cerr)
		}
	}()
	ps, _ := d.compiled.Projector().Stream(s.Name)
	p := newLineParser(ps, d.region)
	sum, status := c.checkInput(d, p, in, o.skip)
	if status != exitOK {
		return status
	}
	for _, id := range ingestTimeVersions(def, s.Name) {
		c.finding(d.region, diag.SeverityWarning, "DF7404",
			id+" groups by ingest_time, so it counts the imported events at the time of the import",
			"a version that groups by event_time counts them at their own time")
	}
	if o.check {
		fmt.Fprintf(&c.out, "%s: %d events fit %s in %s\n", in.name, sum.events, s.Name, d.region)
		return exitOK
	}
	if status = c.checkClock(d); status != exitOK {
		return status
	}
	if sum.toWrite == 0 {
		fmt.Fprintf(&c.out, "%s: no events after line %d\n", in.name, o.skip)
		return exitOK
	}
	c.errorf("plan: import %d events of %s into %s in %s, at most %d a second; an import can't be undone",
		sum.toWrite, in.name, s.Name, d.region, o.rate)
	if o.file == "-" {
		c.terminal = false // stdin held the events, so it can't answer
	}
	question := fmt.Sprintf("import %d events into %s in %s?", sum.toWrite, s.Name, d.region)
	if status = c.confirm("import", question, o.yes); status != exitOK {
		return status
	}
	w := &importWriter{c: c, d: d, p: p, in: in, skip: o.skip, rate: o.rate, lines: sum.lines}
	return w.run()
}

// importInput is the file of events, read once to check it and once to write.
type importInput struct {
	name string // as diagnostics print it
	path string
	gzip bool
	temp bool // a copy of standard input, removed at the end
}

// openInput finds the input that -file names. Standard input is copied to a temporary
// file, so that it can be read twice.
func (c *cli) openInput(file string) (*importInput, error) {
	if file != "-" {
		path := file
		if !filepath.IsAbs(path) {
			path = filepath.Join(c.workdir, path)
		}
		if _, err := os.Stat(path); err != nil {
			if pe, ok := errors.AsType[*fs.PathError](err); ok {
				err = pe.Err
			}
			return nil, fmt.Errorf("-file %s: %w", file, err)
		}
		return &importInput{name: file, path: path, gzip: strings.HasSuffix(file, ".gz")}, nil
	}
	f, err := os.CreateTemp("", "dynaflux-import-*.jsonl")
	if err != nil {
		return nil, fmt.Errorf("copy standard input: %w", err)
	}
	in := &importInput{name: "stdin", path: f.Name(), temp: true}
	_, err = io.Copy(f, c.stdin)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("copy standard input: %w", err), in.close())
	}
	return in, nil
}

func (in *importInput) close() error {
	if !in.temp {
		return nil
	}
	return os.Remove(in.path)
}

// each calls fn with each line's number, from 1, and text, until fn returns false.
func (in *importInput) each(fn func(line int, data []byte) bool) error {
	f, err := os.Open(in.path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if in.gzip {
		gz, gerr := gzip.NewReader(f)
		if gerr != nil {
			return fmt.Errorf("%s: %w", in.name, gerr)
		}
		r = gz
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, maxLineSize)
	line := 0
	for sc.Scan() {
		line++
		if !fn(line, sc.Bytes()) {
			return nil
		}
	}
	if errors.Is(sc.Err(), bufio.ErrTooLong) {
		return fmt.Errorf("%s:%d: the line is over %d MiB, more than any event needs", in.name, line+1, maxLineMiB)
	}
	if sc.Err() != nil {
		return fmt.Errorf("%s: %w", in.name, sc.Err())
	}
	return nil
}

// inputSummary counts the events of a checked input.
type inputSummary struct {
	events  int // lines that hold an event
	toWrite int // events after -skip
	lines   int // the input's last line
}

// checkInput checks every line of in, and prints each one that can't be imported.
func (c *cli) checkInput(d *deployment, p *lineParser, in *importInput, skip int) (inputSummary, int) {
	// Append over a store that only checks its writes makes every check a write would.
	checker := engine.New(checkOnly{d.clients.data}, engine.Options{Region: d.region, Prefix: d.naming.Prefix})
	var sum inputSummary
	bad := 0
	err := in.each(func(line int, data []byte) bool {
		if c.ctx.Err() != nil {
			return false
		}
		sum.lines = line
		if blank(data) {
			return true
		}
		sum.events++
		if line > skip {
			sum.toWrite++
		}
		ev, code, perr := p.parse(data)
		if perr == nil {
			code, perr = "DF7401", appendError(c.ctx, checker, d.compiled, p.stream.Def.Name, ev)
		}
		if perr != nil {
			bad++
			c.lineError(in, line, code, perr)
		}
		return true
	})
	switch {
	case err != nil:
		c.errorf("dynaflux import: %v", err)
		return sum, exitUsage
	case c.ctx.Err() != nil:
		c.errorf("dynaflux import: interrupted; nothing written")
		return sum, exitErrors
	}
	if bad > 0 {
		c.errorf("dynaflux import: %d of %d events can't be imported; nothing written", bad, sum.events)
		return sum, exitErrors
	}
	return sum, exitOK
}

// appendError returns why e can't append ev, or nil.
func appendError(ctx context.Context, e *engine.Engine, cat *engine.Catalog, stream string, ev []expr.Value) error {
	res, err := e.Append(ctx, cat, stream, [][]expr.Value{ev}, engine.AppendMode{Import: true})
	if err != nil {
		return err
	}
	return res[0].Err
}

// checkOnly is a store whose writes only check what DynamoDB would refuse.
type checkOnly struct{ store.Backend }

func (checkOnly) Put(_ context.Context, _ string, it store.Item, c store.Cond) error {
	return store.CheckPut(it, c)
}

func (c *cli) lineError(in *importInput, line int, code string, err error) {
	c.errorf("%s", cell(fmt.Sprintf("%s:%d: error %s: %s", in.name, line, code, engineError(err))))
}

func blank(data []byte) bool { return len(bytes.TrimSpace(data)) == 0 }

// ingestTimeVersions returns the deployed view versions of stream whose keys read
// ingest_time.
func ingestTimeVersions(def *catalog.Catalog, stream string) []string {
	var ids []string
	for _, v := range def.Views {
		for _, vv := range v.Versions {
			names := map[string]bool{}
			for _, k := range vv.Keys {
				k.Expr.ColumnNames(names)
			}
			if vv.Stream == stream && names[catalog.ColumnIngestTime] {
				ids = append(ids, catalog.VersionID(v.Name, vv.Number))
			}
		}
	}
	return ids
}

// checkClock refuses a machine whose clock is more than maxClockSkew off DynamoDB's, by
// the Date of DynamoDB's latest response.
func (c *cli) checkClock(d *deployment) int {
	server, local, ok := d.clients.clock.lastDate()
	if !ok {
		c.errorf("dynaflux import: %s: DynamoDB's responses have no Date, so this machine's clock can't be checked", d.region)
		return exitUsage
	}
	// Date has whole seconds, so a clock in step is up to a second ahead of it.
	skew := local.Sub(server)
	if skew <= maxClockSkew && skew >= -maxClockSkew {
		return exitOK
	}
	how := "ahead of"
	if skew < 0 {
		how, skew = "behind", -skew
	}
	c.finding(d.region, diag.SeverityError, "DF7405",
		fmt.Sprintf("this machine's clock is %s %s DynamoDB's, more than %s", skew.Round(time.Second), how, maxClockSkew),
		"set the clock, with NTP, and run import again")
	return exitErrors
}

// lineParser reads the events of a stream from JSON objects.
type lineParser struct {
	stream *projector.Stream
	region string
	// index is the position in an event of each declared column and event_time.
	index map[string]int
}

func newLineParser(s *projector.Stream, region string) *lineParser {
	p := &lineParser{stream: s, region: region, index: map[string]int{catalog.ColumnEventTime: len(s.Columns)}}
	for i, col := range s.Columns {
		p.index[col.Name] = i
	}
	return p
}

// parse returns the event that data holds: the declared columns' values in catalog order,
// then event_time. A failure comes with its code.
func (p *lineParser) parse(data []byte) ([]expr.Value, string, error) {
	cols := p.stream.Columns
	ev := make([]expr.Value, len(cols)+1)
	seen := make([]bool, len(ev))
	code := "DF7401"
	err := eachMember(data, func(key string, raw json.RawMessage) error {
		i, ok := p.index[key]
		switch {
		case !ok:
			code = "DF7402"
			return p.unknownKey(key)
		case seen[i]:
			return fmt.Errorf("%s appears twice", key)
		case i == len(cols) && bytes.Equal(bytes.TrimSpace(raw), []byte("null")):
			return errors.New("event_time is null; import needs each event's time")
		}
		t := catalog.EventTimeType()
		if i < len(cols) {
			t = cols[i].Type
		}
		v, err := valuetext.ParseJSON(t, raw)
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		ev[i], seen[i] = v, true
		return nil
	})
	if err != nil {
		return nil, code, err
	}
	for i, col := range cols {
		switch {
		case seen[i]:
		case col.CanMiss:
			ev[i] = col.Missing
		default:
			return nil, code, fmt.Errorf("%s is missing; it is neither Nullable nor has a DEFAULT", col.Name)
		}
	}
	if !seen[len(cols)] {
		return nil, code, errors.New("event_time is missing; import needs each event's time")
	}
	return ev, "", nil
}

// eachMember calls fn with each member of the JSON object that data holds, until fn fails.
func eachMember(data []byte, fn func(key string, raw json.RawMessage) error) error {
	if !utf8.Valid(data) {
		return errors.New("the line isn't valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return errors.New("want a JSON object")
	}
	for dec.More() {
		key, err := dec.Token()
		var raw json.RawMessage
		if err == nil {
			err = dec.Decode(&raw)
		}
		if err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		if err = fn(fmt.Sprint(key), raw); err != nil { // a key's token is its string
			return err
		}
	}
	if _, err := dec.Token(); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF // the object isn't closed
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("more than one JSON value on the line")
	}
	return nil
}

func (p *lineParser) unknownKey(key string) error {
	if p.stream.Index(key) >= 0 {
		return fmt.Errorf("%s is set by the import; leave it out", key)
	}
	msg := fmt.Sprintf("%s has no column %s in %s", p.stream.Def.Name, key, p.region)
	if s, ok := diag.Suggest(key, slices.Sorted(maps.Keys(p.index))); ok {
		msg += "; did you mean " + s + "?"
	}
	return errors.New(msg)
}

// importWriter writes the events of a checked input.
type importWriter struct {
	c          *cli
	d          *deployment
	p          *lineParser
	in         *importInput
	skip, rate int
	lines      int // the input's last line

	stored, present, conflicts int
	// through is the last line up to which every event is written or reported; next and
	// done order the writes that end out of turn.
	through int
	next    int
	done    map[int]int // line by sequence number
}

// importJob is an event to write, the seq'th one sent.
type importJob struct {
	seq, line int
	event     []expr.Value
}

type importResult struct {
	importJob

	res engine.Appended
}

// run writes the events after the skipped lines with importWriters writers, paced at rate.
// An interrupt stops sending, and the writes under way end.
func (w *importWriter) run() int {
	c := w.c
	w.through, w.done = w.skip, map[int]int{}
	sendCtx, stopSending := context.WithCancel(c.ctx)
	defer stopSending()
	jobs := make(chan importJob)
	results := make(chan importResult)
	var sendErr error
	go func() {
		defer close(jobs)
		sendErr = w.send(sendCtx, jobs)
	}()
	writeCtx := context.WithoutCancel(c.ctx)
	var wg sync.WaitGroup
	for range importWriters {
		wg.Go(func() {
			for j := range jobs {
				res, err := w.d.engine.Append(writeCtx, w.d.compiled, w.p.stream.Def.Name, [][]expr.Value{j.event}, engine.AppendMode{Import: true})
				r := importResult{importJob: j, res: engine.Appended{Err: err}}
				if err == nil {
					r.res = res[0]
				}
				results <- r
			}
		})
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	tick := time.NewTicker(importProgress)
	defer tick.Stop()
	var failure error
	last := 0 // events done at the last progress line
	for {
		select {
		case r, ok := <-results:
			if !ok {
				return w.finish(cmp.Or(failure, sendErr))
			}
			if err := w.record(r); err != nil && failure == nil {
				failure = fmt.Errorf("%s: line %d: %s", w.d.region, r.line, engineError(err))
				stopSending()
			}
		case <-tick.C:
			n := w.stored + w.present + w.conflicts
			c.errorf("through line %d of %d: %s; %d events a second", w.through, w.lines, w.counts(), (n-last)/int(importProgress/time.Second))
			c.flush()
			last = n
		}
	}
}

// send parses the events after the skipped lines again and sends them to jobs, at most
// rate a second, until ctx ends.
func (w *importWriter) send(ctx context.Context, jobs chan<- importJob) error {
	interval := time.Second / time.Duration(w.rate)
	next := time.Now()
	seq := 0
	var parseErr error
	err := w.in.each(func(line int, data []byte) bool {
		if line <= w.skip || blank(data) {
			return true
		}
		ev, _, err := w.p.parse(data)
		if err != nil {
			parseErr = fmt.Errorf("%s changed after the check: line %d: %w", w.in.name, line, err)
			return false
		}
		if wait := time.Until(next); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return false
			}
		}
		select {
		case jobs <- importJob{seq, line, ev}:
		case <-ctx.Done():
			return false
		}
		seq++
		if now := time.Now(); now.After(next) {
			next = now
		}
		next = next.Add(interval)
		return true
	})
	return cmp.Or(parseErr, err)
}

// record counts a write's result, and returns its error when the import must stop.
func (w *importWriter) record(r importResult) error {
	err := r.res.Err
	switch {
	case err == nil:
		w.stored++
	case errors.Is(err, engine.ErrDuplicate) && r.res.SamePayload:
		w.present++
	case errors.Is(err, engine.ErrDuplicate):
		w.conflicts++
		w.c.lineError(w.in, r.line, "DF7403", errors.New("an event with this line's key is stored with other values"))
		w.c.flush()
	default:
		return err
	}
	w.done[r.seq] = r.line
	for line, ok := w.done[w.next]; ok; line, ok = w.done[w.next] {
		delete(w.done, w.next)
		w.through = line
		w.next++
	}
	return nil
}

func (w *importWriter) counts() string {
	return fmt.Sprintf("%d stored, %d already present, %d conflicting", w.stored, w.present, w.conflicts)
}

// finish prints the outcome and returns the exit status.
func (w *importWriter) finish(failure error) int {
	c := w.c
	n := w.stored + w.present + w.conflicts
	fmt.Fprintf(&c.out, "%d events: %s\n", n, w.counts())
	switch {
	case failure != nil:
		c.errorf("dynaflux import: %v\n  hint: %s", failure, w.resume())
		return exitUsage
	case c.ctx.Err() != nil:
		c.errorf("dynaflux import: interrupted\n  hint: %s", w.resume())
		return exitErrors
	case w.conflicts > 0:
		return exitErrors
	}
	return exitOK
}

// resume says how to resume an import that stopped. Writes that ended out of turn stored
// lines after the last line done, which a stream without DEDUP BY would store again.
func (w *importWriter) resume() string {
	hint := fmt.Sprintf("the lines through %d are done", w.through)
	if len(w.done) == 0 || len(w.p.stream.Def.DedupBy) > 0 {
		return fmt.Sprintf("%s; resume with -skip %d", hint, w.through)
	}
	return fmt.Sprintf("%s, and lines %s are stored too; %s has no DEDUP BY, so empty those lines in the input "+
		"before you resume with -skip %d, or their events are stored twice",
		hint, lineRanges(slices.Sorted(maps.Values(w.done))), w.p.stream.Def.Name, w.through)
}

// lineRanges writes sorted line numbers as ranges, such as "6-9, 12".
func lineRanges(lines []int) string {
	var parts []string
	for i := 0; i < len(lines); {
		j := i
		for j+1 < len(lines) && lines[j+1] == lines[j]+1 {
			j++
		}
		part := strconv.Itoa(lines[i])
		if j > i {
			part += "-" + strconv.Itoa(lines[j])
		}
		parts = append(parts, part)
		i = j + 1
	}
	return strings.Join(parts, ", ")
}
