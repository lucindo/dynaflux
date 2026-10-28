// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/meta"
)

// hashText is how many hex digits of a catalog hash the text shows.
const hashText = 12

// printStatus prints the versions by region, then each region's catalog, streams and jobs.
func (c *cli) printStatus(f *fleet, rs []*regionStatus, verbose bool) {
	c.printVersions(rs)
	for _, r := range rs {
		c.printRegion(f, r, verbose)
	}
}

// printVersions prints a table of the versions that a region's catalog defines or that
// block its projectors, by region.
func (c *cli) printVersions(rs []*regionStatus) {
	var ids []string
	for _, r := range rs {
		if r.cat == nil {
			continue
		}
		for _, id := range r.versionIDs() {
			if _, defined := r.cat.Version(id); (defined || r.blocking(id)) && !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		return
	}
	slices.SortFunc(ids, compareVersions)
	rows := [][]string{{"version"}}
	for _, r := range rs {
		rows[0] = append(rows[0], r.region)
	}
	for _, id := range ids {
		row := []string{id}
		for _, r := range rs {
			row = append(row, r.cell(id))
		}
		rows = append(rows, row)
	}
	alignColumns(&c.out, rows)
	c.out.WriteString("\n")
}

// cell is version id's state in the table: "-" where the region doesn't have it, and "?"
// where its catalog couldn't be read.
func (r *regionStatus) cell(id string) string {
	switch {
	case r.cat == nil && r.notDeployed:
		return "-"
	case r.cat == nil:
		return "?"
	}
	if _, defined := r.cat.Version(id); defined || r.blocking(id) {
		return string(r.state(id).State)
	}
	return "-"
}

func (c *cli) printRegion(f *fleet, r *regionStatus, verbose bool) {
	switch {
	case r.err != nil:
		fmt.Fprintf(&c.out, "%s: couldn't be read\n", r.region)
		return
	case r.notDeployed:
		fmt.Fprintf(&c.out, "%s: not deployed: no catalog in %s\n", r.region, f.naming.MetaTable())
		return
	}
	fmt.Fprintf(&c.out, "%s: catalog %s (dynaflux %s), registered %s\n", r.region, r.reg.Hash[:hashText], r.reg.Generator, timeText(r.reg.At))
	if r.cat == nil {
		return
	}
	if r.local {
		c.out.WriteString("  mappings, metrics, gaps and the jobs' executions aren't available locally\n")
	}
	for _, s := range r.streams {
		c.printStream(r, s, verbose)
	}
	for _, j := range r.jobs {
		if j.State != meta.JobDone {
			c.printJob(r, j)
		}
	}
	for _, id := range r.versionIDs() {
		if vs := r.state(id); vs.State == meta.Live && !vs.PartialSince.IsZero() {
			fmt.Fprintf(&c.out, "  note: %s is partial since %s: it misses the events before then\n", id, timeText(vs.PartialSince))
		}
	}
	c.printGapNotes(f, r)
}

// printGapNotes notes, by stream, the records Lambda discarded that no version misses, and
// the records status didn't read.
func (c *cli) printGapNotes(f *fleet, r *regionStatus) {
	for _, s := range r.streams {
		var records, batches int
		var first, last time.Time
		for _, g := range s.gaps {
			if len(r.missing(s.name, g)) > 0 {
				continue
			}
			records, batches = records+g.Records, batches+1
			if first.IsZero() || g.FirstArrival.Before(first) {
				first = g.FirstArrival
			}
			if g.LastArrival.After(last) {
				last = g.LastArrival
			}
		}
		if batches > 0 {
			fmt.Fprintf(&c.out, "  note: Lambda discarded %d records of stream %s in %d %s from %s to %s, which no version misses; the region's archive misses them\n",
				records, s.name, batches, plural(batches, "batch", "batches"), timeText(first), timeText(last))
		}
	}
	where := "s3://" + f.naming.Bucket(f.account, r.region) + "/" + deployed.GapPrefix
	switch {
	case r.gapsUnread:
		fmt.Fprintf(&c.out, "  gaps couldn't be read from %s\n", where)
	case r.cappedGaps > 0:
		fmt.Fprintf(&c.out, "  note: %d older on-failure %s in %s weren't read: status reads the newest %d\n",
			r.cappedGaps, plural(r.cappedGaps, "record", "records"), where, deployed.MaxGaps)
	}
	if r.olderGaps > 0 {
		fmt.Fprintf(&c.out, "  note: %d on-failure %s in %s %s every version's activation, so no version misses %s\n", r.olderGaps,
			plural(r.olderGaps, "record", "records"), where, plural(r.olderGaps, "predates", "predate"), plural(r.olderGaps, "it", "them"))
	}
}

func (c *cli) printStream(r *regionStatus, s streamStatus, verbose bool) {
	switch {
	case !r.local:
		fmt.Fprintf(&c.out, "  stream %s: %s\n", s.name, s.delivery())
	case s.check != nil:
		fmt.Fprintf(&c.out, "  stream %s: archive %s\n", s.name, s.check.text())
	case verbose:
		fmt.Fprintf(&c.out, "  stream %s:\n", s.name)
	}
	for _, u := range s.units {
		line := "    unit " + u.ID + ": no progress"
		if u.Progress != "" {
			line = "    unit " + u.ID + ": progress " + u.Progress + " at " + timeText(u.ProgressAt)
		}
		var fences []string
		for _, v := range slices.SortedFunc(maps.Keys(u.Fences), compareVersions) {
			fences = append(fences, v+" "+u.Fences[v])
		}
		if len(fences) > 0 {
			line += "; fences " + strings.Join(fences, ", ")
		}
		c.out.WriteString(line + "\n")
	}
}

// delivery describes the stream's lag, its mappings and its archive.
func (s *streamStatus) delivery() string {
	parts := []string{"no recent batches"}
	switch {
	case s.lagUnread:
		parts[0] = "lag couldn't be read"
	case s.lag != nil:
		parts[0] = "lag " + durationText(*s.lag)
	}
	if !s.mapped {
		parts = append(parts, "mappings couldn't be read")
	}
	for _, m := range s.mappings {
		p := "mapping " + m.UUID + " to " + m.Function + " " + strings.ToLower(m.State)
		if m.LastResult != "" {
			p += ", last result " + m.LastResult
		}
		parts = append(parts, p)
	}
	if s.archive == "" {
		return strings.Join(parts, "; ")
	}
	archive := "archive without recent data"
	switch {
	case s.archiveUnread:
		archive = "archive couldn't be read"
	case s.delivered != nil:
		archive = "archive delivered through " + timeText(*s.delivered)
	}
	if s.check != nil {
		archive += ", " + s.check.text()
	}
	return strings.Join(append(parts, archive), "; ")
}

// text says whether the archive is complete, since when, or the rule that failed.
func (a *archiveCheck) text() string {
	switch {
	case a.unread:
		return "completeness couldn't be read"
	case !a.verdict.Complete:
		return "not complete: " + a.verdict.Reason
	case a.unchecked.IsZero():
		return "complete since " + timeText(a.since)
	}
	return "complete since " + timeText(a.since) + ", not checked before " + a.unchecked.UTC().Format(time.DateOnly)
}

func (c *cli) printJob(r *regionStatus, j meta.Job) {
	line := fmt.Sprintf("  backfill %s: %s, %s", j.Version, j.State, progress(j))
	if j.State == meta.JobRunning {
		line += "; rate " + rateText(j.Rate)
		if left := j.Staged - j.Applied - j.Skipped; j.Rate > 0 && left > 0 {
			line += ", about " + durationText(time.Duration(left/int64(j.Rate))*time.Second) + " left"
		}
	}
	line += "; " + describeSplit(j.Split)
	if vs := r.state(j.Version); j.Split == meta.SplitTime && !vs.SplitAt.IsZero() {
		line += " at " + timeText(vs.SplitAt)
	}
	if j.Split != meta.SplitFence && r.cat.ExactHandoff(r.region, j.Stream) {
		line += " (the archive wasn't complete when the job started, or -source table chose it)"
	}
	if n := j.Archive.Scanned; n > 0 {
		line += "; Athena scanned " + bytesText(n)
	}
	line += r.activity(j)
	e, read := r.execs[j.Version]
	switch {
	case j.State != meta.JobRunning:
	case r.local:
		line += "; execution not available locally"
	case !read:
		line += "; execution couldn't be read"
	case e.ARN == "":
		line += "; interrupted"
	case e.Continuation:
		line += "; continuing in " + e.ARN[strings.LastIndex(e.ARN, ":")+1:]
	default:
		line += "; execution running"
	}
	c.out.WriteString(line + jobError(j) + "\n")
}

// jobActivity says why a job that hasn't ended doesn't move: it waits for Firehose, or for the
// projector's lag to fall.
func jobActivity(j meta.Job) string {
	switch {
	case j.State.Ended():
	case j.State == meta.JobRunning && j.Split == meta.SplitFence && j.Phase == meta.PhaseWatermark:
		return "; waits for Firehose to deliver the records taken before " + timeText(j.Cut)
	case j.Paused && j.State == meta.JobRunning:
		return "; paused: the projector is more than 5 minutes behind"
	}
	return ""
}

// idleSlack is how long past backfill.IdleAfter a job may still wait at watermark before
// status says it can't tell that Firehose holds nothing: the job checks every minute.
const idleSlack = 2 * time.Minute

// activity is jobActivity, except that it says from the archive's metrics why a job still waits
// at watermark.
func (r *regionStatus) activity(j meta.Job) string {
	s := r.stream(j.Stream)
	if j.State != meta.JobRunning || j.Split != meta.SplitFence || j.Phase != meta.PhaseWatermark ||
		s == nil || s.archive == "" || s.archiveUnread || r.local {
		return jobActivity(j)
	}
	switch idle := j.Cut.Add(backfill.IdleAfter); {
	case s.delivered != nil:
		return fmt.Sprintf("; waits for Firehose, delivered through %s, to deliver the records taken before %s", timeText(*s.delivered), timeText(j.Cut))
	case r.now.Before(idle.Add(idleSlack)):
		return "; Firehose shows no recent data: the job waits until " + timeText(idle) + " to tell that it holds nothing"
	}
	return "; Firehose shows no recent data, and the job can't tell that it holds nothing: CloudWatch doesn't list its data " +
		"freshness, as after 14 days without records, or the delivery stream isn't ACTIVE; append an event to " + j.Stream +
		", or rebuild the view to fill it from the table"
}

// bytesText is n bytes in decimal units, as Athena bills them.
func bytesText(n int64) string {
	const unit = 1000
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
}

// statusJSON is status's -json document.
type statusJSON struct {
	Format   int           `json:"format"`
	Store    string        `json:"store"`
	Prefix   string        `json:"prefix"`
	Account  string        `json:"account,omitzero"`
	Endpoint string        `json:"endpoint,omitzero"`
	Regions  []regionJSON  `json:"regions"`
	Findings []findingJSON `json:"findings"`
}

type regionJSON struct {
	Region   string        `json:"region"`
	Error    string        `json:"error,omitzero"`
	Unread   []string      `json:"unread,omitzero"` // the parts that couldn't be read
	Deployed bool          `json:"deployed"`
	Catalog  *catalogJSON  `json:"catalog,omitzero"`
	Versions []versionJSON `json:"versions,omitzero"`
	Jobs     []jobJSON     `json:"jobs,omitzero"`
	Streams  []streamJSON  `json:"streams,omitzero"`
	// OlderGaps and CappedGaps count Lambda's records not read (lifecycle.md §12).
	OlderGaps  int `json:"older_gaps,omitzero"`
	CappedGaps int `json:"capped_gaps,omitzero"`
}

type catalogJSON struct {
	Hash         string    `json:"hash"`
	Generator    string    `json:"generator"`
	Format       int       `json:"format"`
	RegisteredAt time.Time `json:"registered_at"`
}

type versionJSON struct {
	Version      string       `json:"version"`
	Stream       string       `json:"stream,omitzero"`
	State        string       `json:"state"`
	Split        string       `json:"split,omitzero"`
	ActivatedAt  time.Time    `json:"activated_at,omitzero"`
	SplitAt      time.Time    `json:"split_at,omitzero"`
	PartialSince time.Time    `json:"partial_since,omitzero"`
	RetiredAt    time.Time    `json:"retired_at,omitzero"`
	Failure      *failureJSON `json:"failure,omitzero"`
}

type failureJSON struct {
	Cause   string    `json:"cause"`
	Unit    string    `json:"unit,omitzero"`
	Seq     string    `json:"seq,omitzero"`
	EventID string    `json:"event_id,omitzero"`
	Error   string    `json:"error"`
	At      time.Time `json:"at"`
}

// The execution_status of a running job: what runs it.
const (
	statusRunning     = "running"
	statusInterrupted = "interrupted"
)

type jobJSON struct {
	Version   string        `json:"version"`
	State     string        `json:"state"`
	Phase     string        `json:"phase"`
	Attempt   int           `json:"attempt"`
	Split     string        `json:"split"`
	Staged    int64         `json:"staged"`
	Applied   int64         `json:"applied"`
	Skipped   int64         `json:"skipped"`
	Rate      int           `json:"rate"`
	Paused    bool          `json:"paused"`
	Scanned   int64         `json:"scanned_bytes,omitzero"` // what Athena scanned for an archive source
	UpdatedAt time.Time     `json:"updated_at"`
	Execution string        `json:"execution"`
	Running   string        `json:"running,omitzero"` // the execution that runs the job
	Status    string        `json:"execution_status,omitzero"`
	Error     *jobErrorJSON `json:"error,omitzero"`
}

type jobErrorJSON struct {
	Phase   string    `json:"phase"`
	Message string    `json:"message"`
	At      time.Time `json:"at"`
}

type streamJSON struct {
	Stream     string        `json:"stream"`
	ARN        string        `json:"arn,omitzero"`
	LagMS      *int64        `json:"lag_ms,omitzero"`
	Mappings   []mappingJSON `json:"mappings,omitzero"`
	Archive    string        `json:"archive,omitzero"`
	FreshnessS *int64        `json:"archive_freshness_s,omitzero"`
	Delivered  time.Time     `json:"archive_delivered_through,omitzero"`
	FailedRows int64         `json:"archive_failed_rows,omitzero"`
	Complete   *completeJSON `json:"archive_completeness,omitzero"`
	Gaps       []gapJSON     `json:"gaps,omitzero"`
	Units      []unitJSON    `json:"units,omitzero"`
}

// completeJSON is whether the region's archive of a stream with events: all is complete.
type completeJSON struct {
	Unread          bool      `json:"unread,omitzero"`
	Complete        bool      `json:"complete"`
	Since           time.Time `json:"since,omitzero"` // the origin's time
	UncheckedBefore time.Time `json:"unchecked_before,omitzero"`
	Rule            int       `json:"rule,omitzero"` // the rule that failed
	Reason          string    `json:"reason,omitzero"`
	At              time.Time `json:"at,omitzero"`
	Suspect         bool      `json:"suspected_gap,omitzero"`
}

func (a *archiveCheck) json() *completeJSON {
	v := a.verdict
	return &completeJSON{Unread: a.unread, Complete: v.Complete, Since: a.since.UTC(), UncheckedBefore: a.unchecked.UTC(), Rule: v.Rule,
		Reason: v.Reason, At: v.At.UTC(), Suspect: v.Suspect}
}

type mappingJSON struct {
	UUID       string `json:"uuid"`
	Function   string `json:"function"`
	State      string `json:"state"`
	LastResult string `json:"last_result,omitzero"`
}

type gapJSON struct {
	Object       string    `json:"object"`
	Unit         string    `json:"unit"`
	FirstSeq     string    `json:"first_seq"`
	LastSeq      string    `json:"last_seq"`
	FirstArrival time.Time `json:"first_arrival"`
	LastArrival  time.Time `json:"last_arrival"`
	Records      int       `json:"records"`
	Missing      []string  `json:"missing"`
}

type unitJSON struct {
	Unit       string            `json:"unit"`
	Progress   string            `json:"progress,omitzero"`
	ProgressAt time.Time         `json:"progress_at,omitzero"`
	Fences     map[string]string `json:"fences,omitzero"`
}

func statusJSONOf(f *fleet, rs []*regionStatus, fs []finding) statusJSON {
	doc := statusJSON{Format: 1, Store: f.naming.Store, Prefix: f.naming.Prefix, Account: f.account, Endpoint: f.endpoint,
		Regions: []regionJSON{}, Findings: []findingJSON{}}
	for _, r := range rs {
		doc.Regions = append(doc.Regions, r.json())
	}
	for _, fd := range fs {
		doc.Findings = append(doc.Findings, fd.json())
	}
	return doc
}

func (r *regionStatus) json() regionJSON {
	j := regionJSON{Region: r.region, Deployed: !r.notDeployed && r.err == nil, OlderGaps: r.olderGaps, CappedGaps: r.cappedGaps}
	if r.err != nil {
		j.Error = r.err.Error()
	}
	for _, err := range r.unread {
		j.Unread = append(j.Unread, err.Error())
	}
	if r.reg.Hash != "" {
		j.Catalog = &catalogJSON{Hash: r.reg.Hash, Generator: r.reg.Generator, Format: r.reg.Format, RegisteredAt: r.reg.At.UTC()}
	}
	if r.cat == nil {
		return j
	}
	for _, id := range r.versionIDs() {
		vs := r.state(id)
		v := versionJSON{Version: id, Stream: vs.Stream, State: string(vs.State), Split: string(vs.Split),
			ActivatedAt: vs.ActivatedAt.UTC(), SplitAt: vs.SplitAt.UTC(), PartialSince: vs.PartialSince.UTC(), RetiredAt: vs.RetiredAt.UTC()}
		if vv, ok := r.cat.Version(id); ok && v.Stream == "" {
			v.Stream = vv.Stream
		}
		if fl := vs.Failure; fl != nil {
			v.Failure = &failureJSON{Cause: string(fl.Cause), Unit: fl.Unit, Seq: fl.Seq, EventID: fl.EventID, Error: fl.Error, At: fl.At.UTC()}
		}
		j.Versions = append(j.Versions, v)
	}
	for _, job := range r.jobs {
		j.Jobs = append(j.Jobs, r.jobJSON(job))
	}
	for _, s := range r.streams {
		j.Streams = append(j.Streams, r.streamJSON(s))
	}
	return j
}

func (r *regionStatus) jobJSON(job meta.Job) jobJSON {
	j := jobJSONOf(job)
	if e, read := r.execs[job.Version]; read {
		j.Running, j.Status = e.ARN, statusInterrupted
		switch {
		case e.Continuation:
			j.Status = "continuing"
		case e.ARN != "":
			j.Status = statusRunning
		}
	}
	return j
}

// jobJSONOf is job's item in -json output.
func jobJSONOf(job meta.Job) jobJSON {
	j := jobJSON{Version: job.Version, State: string(job.State), Phase: string(job.Phase), Attempt: job.Attempt, Split: string(job.Split),
		Staged: job.Staged, Applied: job.Applied, Skipped: job.Skipped, Rate: job.Rate, Paused: job.Paused,
		Scanned: job.Archive.Scanned, UpdatedAt: job.UpdatedAt.UTC(), Execution: job.Execution}
	if e := job.Error; e != nil {
		j.Error = &jobErrorJSON{Phase: string(e.Phase), Message: e.Message, At: e.At.UTC()}
	}
	return j
}

func (r *regionStatus) streamJSON(s streamStatus) streamJSON {
	j := streamJSON{Stream: s.name, ARN: s.arn, Archive: s.archive, FailedRows: s.failed}
	if s.lag != nil {
		j.LagMS = new(s.lag.Milliseconds())
	}
	if s.freshness != nil {
		j.FreshnessS, j.Delivered = new(int64(s.freshness.Seconds())), s.delivered.UTC()
	}
	if s.check != nil {
		j.Complete = s.check.json()
	}
	for _, m := range s.mappings {
		j.Mappings = append(j.Mappings, mappingJSON{UUID: m.UUID, Function: m.Function, State: m.State, LastResult: m.LastResult})
	}
	for _, g := range s.gaps {
		j.Gaps = append(j.Gaps, gapJSON{Object: g.Key, Unit: g.Shard, FirstSeq: g.FirstSeq, LastSeq: g.LastSeq,
			FirstArrival: g.FirstArrival, LastArrival: g.LastArrival, Records: g.Records, Missing: nonNil(r.missing(s.name, g))})
	}
	for _, u := range s.units {
		j.Units = append(j.Units, unitJSON{Unit: u.ID, Progress: u.Progress, ProgressAt: u.ProgressAt.UTC(), Fences: u.Fences})
	}
	return j
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// writeJSON prints v as one indented JSON document.
func (c *cli) writeJSON(v any) int {
	enc := json.NewEncoder(&c.out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		c.errorf("dynaflux: %v", err)
		return exitUsage
	}
	return exitOK
}
