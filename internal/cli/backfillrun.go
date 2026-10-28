// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/meta"
)

// executionInput is the input of a job's execution, with every field the state machine
// reads: JSONata reads a missing field as undefined, not as an error.
type executionInput struct {
	Job     string `json:"job"`
	Attempt int    `json:"attempt"`
	Name    string `json:"name"`
	K       int    `json:"k"`
	Steps   int    `json:"steps"`
}

// act carries out the plans: it starts and resumes jobs in executions of their own, changes
// rates and cancels. It prints what it did, on stdout or with -json on stderr, and returns
// exitUsage when an AWS call failed.
func (c *cli) act(f *fleet, o backfillOptions, plans []*regionPlan) int {
	out := &c.out
	if o.json {
		out = &c.errOut
	}
	status := exitOK
	for i, p := range plans {
		d := f.regions[i]
		for _, it := range p.items {
			msg, err := c.actOn(f, d, o, it)
			if err != nil {
				c.errorf("dynaflux backfill: %s: %s: %v", p.region, it.version, err)
				it.failed, status = true, exitUsage
				continue
			}
			if msg != "" {
				fmt.Fprintf(out, "%s: %s: %s\n", p.region, it.version, msg)
			}
		}
	}
	return status
}

// actOn carries out one item and says what it did, or "" when it did nothing.
func (c *cli) actOn(f *fleet, d *deployment, o backfillOptions, it *planItem) (string, error) {
	switch it.act {
	case actStart, actResume:
		name, stream := it.job.Name, it.job.Stream
		if it.act == actStart {
			name, stream = executionName(it.version, time.Now()), it.stream
		}
		j, err := backfill.Start(c.ctx, d.meta, backfill.Request{
			Version: it.version, Stream: stream, Name: name, Split: it.split, Rate: o.rate.value,
			Execution: func(attempt int) string { return f.execution(d.region, attemptName(name, attempt)) },
		})
		if err == nil && it.act == actResume && o.rate.set {
			err = backfill.SetRate(c.ctx, d.meta, it.version, o.rate.value)
		}
		if err != nil {
			return "", err
		}
		did := "started"
		if it.act == actResume {
			did = "resumed as attempt " + strconv.Itoa(j.Attempt)
		}
		return c.startExecution(f, d, j, did)
	case actRunning:
		if !o.rate.set {
			return "", nil
		}
		return "rate set to " + rateText(o.rate.value), backfill.SetRate(c.ctx, d.meta, it.version, o.rate.value)
	case actCancel:
		return c.cancel(d, it)
	case actLive, actRefuse, actNone:
	}
	return "", nil
}

// cancel cancels the job of it, then stops its executions and the Athena query that the job,
// as cancelled, last recorded: no step records another after.
func (c *cli) cancel(d *deployment, it *planItem) (string, error) {
	j, err := backfill.Cancel(c.ctx, d.meta, it.version)
	if err != nil {
		return "", err
	}
	for _, arn := range it.stop {
		if _, err = d.clients.sfn.StopExecution(c.ctx, &sfn.StopExecutionInput{ExecutionArn: &arn}); err != nil {
			return "", fmt.Errorf("stop execution %s: %w", arn, err)
		}
	}
	id, what := runningQuery(j)
	if id == "" {
		return string(meta.JobCancelled), nil
	}
	if _, err = d.clients.athena.StopQueryExecution(c.ctx, &athena.StopQueryExecutionInput{QueryExecutionId: &id}); err != nil {
		return "", fmt.Errorf("stop %s, Athena query %s: %w", what, id, err)
	}
	return "cancelled; stopped " + what + ", Athena query " + id, nil
}

// runningQuery returns the Athena query that j's stage holds and may still run, and what it
// does: the listing of the partitions before the plan, or a slice's try before its files.
func runningQuery(j meta.Job) (string, string) {
	a := j.Archive
	switch {
	case j.Split != meta.SplitFence:
	case a.Plan == "" && a.PlanQuery != "":
		return a.PlanQuery, "the listing of the archive's partitions"
	case a.Export.ID != "" && a.Query == "":
		return a.Export.ID, "the archive export"
	}
	return "", ""
}

func attemptName(name string, attempt int) string { return name + "-a" + strconv.Itoa(attempt) }

// startExecution starts the execution of j's attempt. Starting it again with the same name
// and input while it runs does nothing, so a retried run may.
func (c *cli) startExecution(f *fleet, d *deployment, j meta.Job, did string) (string, error) {
	name := attemptName(j.Name, j.Attempt)
	input, err := json.Marshal(executionInput{Job: j.Version, Attempt: j.Attempt, Name: j.Name})
	if err != nil {
		return "", err
	}
	machine, text := f.machine(d.region), string(input)
	if _, err = d.clients.sfn.StartExecution(c.ctx, &sfn.StartExecutionInput{StateMachineArn: &machine, Name: &name, Input: &text}); err != nil {
		return "", fmt.Errorf("start execution %s: %w", name, err)
	}
	return did + " in execution " + name, nil
}

// Watch's polling intervals: the job items are cheap to read, the executions less so.
const (
	watchJobs       = 5 * time.Second
	watchExecutions = 30 * time.Second
)

// watched is a job that -watch follows.
type watched struct {
	d       *deployment
	version string
	last    string // the line printed last
	ended   bool
	ok      bool      // it ended live
	applied int64     // at the previous poll
	at      time.Time // of the previous poll
	// exec is what ran the job when its executions were read last, if checked.
	exec    deployed.Execution
	checked bool
}

// watchHeader is -watch's first JSON line.
type watchHeader struct {
	Format   int    `json:"format"`
	Store    string `json:"store"`
	Prefix   string `json:"prefix"`
	Account  string `json:"account,omitzero"`
	Endpoint string `json:"endpoint,omitzero"`
}

// watchLine is a JSON line of -watch: a job in its region as status -json shows it, or why
// it couldn't be read.
type watchLine struct {
	Region        string  `json:"region"`
	Version       string  `json:"version"`
	Job           jobJSON `json:"job,omitzero"`
	RatePerSecond float64 `json:"rate_per_s,omitzero"`
	ReadError     string  `json:"read_error,omitzero"`
}

// watch follows the jobs that the run started, resumed or left running, printing a line
// when one moves, until each has ended. It returns exitOK when every version is live, and
// exitErrors when one broke, was cancelled, or its job's executions ended without it, or
// when the watch was interrupted, which stops only the watching. A poll that fails is
// reported in place, and tried again.
func (c *cli) watch(f *fleet, plans []*regionPlan, asJSON bool) int {
	var ws []*watched
	for i, p := range plans {
		for _, it := range p.items {
			if (it.act == actStart || it.act == actResume || it.act == actRunning) && !it.failed {
				ws = append(ws, &watched{d: f.regions[i], version: it.version})
			}
		}
	}
	if len(ws) == 0 {
		return exitOK
	}
	if asJSON {
		c.writeJSONLine(watchHeader{Format: 1, Store: f.naming.Store, Prefix: f.naming.Prefix, Account: f.account, Endpoint: f.endpoint})
	}
	var lastExecutions time.Time
	for {
		executions := time.Since(lastExecutions) >= watchExecutions
		if executions {
			lastExecutions = time.Now()
		}
		if c.pollAll(f, ws, executions, asJSON) == 0 {
			if slices.ContainsFunc(ws, func(w *watched) bool { return !w.ok }) {
				return exitErrors
			}
			return exitOK
		}
		select {
		case <-c.ctx.Done():
			c.errorf("dynaflux backfill: stopped watching; the jobs go on")
			return exitErrors
		case <-time.After(watchJobs):
		}
	}
}

// pollAll polls each watched job that hasn't ended, and returns how many are left.
func (c *cli) pollAll(f *fleet, ws []*watched, executions, asJSON bool) int {
	left := 0
	for _, w := range ws {
		if !w.ended {
			c.poll(f, w, executions, asJSON)
		}
		if !w.ended {
			left++
		}
	}
	c.flush()
	return left
}

// poll reads a watched job, and with executions whether its executions still run, and
// prints a line when it changed.
func (c *cli) poll(f *fleet, w *watched, executions, asJSON bool) {
	j, err := w.read(c.ctx)
	executions = executions && err == nil && j.State == meta.JobRunning
	if executions {
		w.exec, err = deployed.Running(c.ctx, w.d.clients.sfn, f.machine(w.d.region), j)
		w.checked = err == nil
	}
	line, wl := w.observe(j, executions, err)
	if asJSON {
		data, merr := json.Marshal(wl)
		if merr != nil {
			c.errorf("dynaflux backfill: %v", merr)
			return
		}
		line = string(data)
	}
	if line != w.last {
		if asJSON {
			c.out.WriteString(line + "\n")
		} else {
			fmt.Fprintf(&c.out, "%s  %s  %s\n", w.d.region, w.version, line)
		}
		w.last = line
	}
}

func (w *watched) read(ctx context.Context) (meta.Job, error) {
	j, ok, err := w.d.meta.Job(ctx, w.version)
	if err == nil && !ok {
		err = meta.ErrNoJob
	}
	if err != nil {
		return meta.Job{}, fmt.Errorf("read the job: %w", err)
	}
	return j, nil
}

// observe takes what a poll read of the job, and of its executions when executions, and
// returns its text line and its JSON line; err is why the poll failed.
func (w *watched) observe(j meta.Job, executions bool, err error) (string, watchLine) {
	wl := watchLine{Region: w.d.region, Version: w.version}
	if err != nil {
		wl.ReadError = err.Error()
		return "couldn't be read: " + err.Error() + "; trying again", wl
	}
	wl.Job = jobJSONOf(j)
	var line string
	switch j.State {
	case meta.JobDone:
		w.ended, w.ok, line = true, true, "live"
	case meta.JobBroken, meta.JobFailed:
		w.ended, line = true, string(j.State)
		if j.Error != nil {
			line += ": " + j.Error.Message
		}
	case meta.JobCancelled:
		w.ended, line = true, "cancelled"
	case meta.JobRunning:
		line = fmt.Sprintf("%s  %d/%d applied", j.Phase, j.Applied, j.Staged)
		if now := time.Now(); !w.at.IsZero() && j.Applied > w.applied {
			rate := float64(j.Applied-w.applied) / now.Sub(w.at).Seconds()
			eta := time.Duration(float64(max(0, j.Staged-j.Applied-j.Skipped))/rate) * time.Second
			line += fmt.Sprintf("  %.0f/s  ETA %s", rate, eta.Round(time.Second))
			wl.RatePerSecond = rate
		}
		w.applied, w.at = j.Applied, time.Now()
		line += jobActivity(j)
		switch {
		case executions && w.checked && w.exec.ARN == "":
			w.ended, line = true, "interrupted: its execution ended; run dynaflux backfill "+w.version+" again to resume it"
			wl.Job.Status = statusInterrupted
		case w.checked:
			wl.Job.Running, wl.Job.Status = w.exec.ARN, statusRunning
		}
	}
	return line, wl
}

// writeJSONLine prints v as one JSON line.
func (c *cli) writeJSONLine(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		c.errorf("dynaflux: %v", err)
		return
	}
	c.out.Write(append(data, '\n'))
}
