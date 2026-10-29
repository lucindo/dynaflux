// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/meta"
)

const (
	// lagWarning is the projector lag that status warns about: the lag alarm's threshold.
	lagWarning = 15 * time.Minute
	// staleArchive is the archive delay that status warns about: the freshness alarm's.
	staleArchive = 15 * time.Minute
	// streamRetention is how long a change stream keeps a record.
	streamRetention = 24 * time.Hour
)

// versionIDs returns the versions that r's catalog defines and those r holds a state of,
// ordered by view and number.
func (r *regionStatus) versionIDs() []string {
	ids := slices.Collect(maps.Keys(r.states))
	for id := range r.cat.Versions() {
		if _, ok := r.states[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, compareVersions)
	return ids
}

// compareVersions orders version IDs by view, then by number.
func compareVersions(a, b string) int {
	av, an, _ := strings.Cut(a, "@v")
	bv, bn, _ := strings.Cut(b, "@v")
	return cmp.Or(strings.Compare(av, bv), cmp.Compare(len(an), len(bn)), strings.Compare(an, bn))
}

// blocking reports whether projectors of r's catalog fail on version id (guard 2): it is
// activating or live, and the catalog has its stream but neither defines nor retires it.
func (r *regionStatus) blocking(id string) bool {
	vs := r.state(id)
	_, defined := r.cat.Version(id)
	_, read := r.cat.Stream(vs.Stream)
	return (vs.State == meta.Activating || vs.State == meta.Live) && read && !defined && !slices.Contains(r.cat.Retired, id)
}

func (r *regionStatus) stream(name string) *streamStatus {
	i := slices.IndexFunc(r.streams, func(s streamStatus) bool { return s.name == name })
	if i < 0 {
		return nil
	}
	return &r.streams[i]
}

// findings returns what needs an action in r. h is the flags that chose the environment,
// for the hints.
func (r *regionStatus) findings(f *fleet, h hintFlags, now time.Time) []finding {
	if r.err != nil || r.notDeployed {
		return nil
	}
	if fd, newer := formatFinding(r.region, r.reg); newer {
		return []finding{fd}
	}
	var fs []finding
	for _, id := range r.versionIDs() {
		vs := r.state(id)
		vv, defined := r.cat.Version(id)
		j, hasJob := r.job(id)
		switch {
		case vs.State == meta.Broken:
			fs = append(fs, r.broken(f, vs, vv, defined, h))
		case r.blocking(id):
			fs = append(fs, r.blocked(vs, now, h))
		case r.stale(id):
			fs = append(fs, regionFinding(r.region, diag.SeverityWarning, "DF7220",
				id+" is no longer current, and still live: projectors still update its table",
				"retire it with "+r.retire(f, id, h)))
		case !defined || vs.State == meta.Live || vs.State == meta.Retired:
		case hasJob && j.State == meta.JobFailed:
			fs = append(fs, regionFinding(r.region, diag.SeverityError, "DF7219",
				fmt.Sprintf("the backfill of %s failed on an archive of stream %s that wasn't complete, so %s can't go live%s", id, vv.Stream, id, jobError(j)),
				h.rebuild([]string{id})))
		case hasJob && j.State == meta.JobRunning:
			if e, read := r.execs[id]; read && e.ARN == "" {
				fs = append(fs, regionFinding(r.region, diag.SeverityWarning, "DF7216",
					fmt.Sprintf("the backfill of %s is interrupted: its execution %s ended without finishing%s", id, j.Execution, jobError(j)),
					"resume it with dynaflux backfill "+id+" -region "+r.region+h.flags()))
			}
		case !hasJob || j.State == meta.JobCancelled:
			msg := id + " needs a backfill"
			if hasJob {
				msg += "; its job was cancelled"
			}
			fs = append(fs, regionFinding(r.region, diag.SeverityWarning, "DF7202", msg, r.fill(id, h)))
		}
	}
	for _, s := range r.streams {
		fs = append(fs, r.archiveFindings(s, h, now)...)
	}
	if r.local {
		return fs
	}
	for _, s := range r.streams {
		fs = append(fs, r.streamFindings(f, s, h)...)
	}
	return append(fs, r.leftOutFindings()...)
}

// leftOutFindings is DF7221 when r's views left out values too long to store.
func (r *regionStatus) leftOutFindings() []finding {
	if r.leftOut == 0 {
		return nil
	}
	return []finding{regionFinding(r.region, diag.SeverityWarning, "DF7221",
		fmt.Sprintf("views left out %d rows or aggregate inputs in the last 24 hours, for values too long to store", r.leftOut),
		"the projectors' batch metrics log lines name the view versions under leftOut")}
}

// fill returns the command that fills version id in r: dev on DynamoDB Local, where backfill
// doesn't run.
func (r *regionStatus) fill(id string, h hintFlags) string {
	if h.endpoint != "" {
		return "run " + h.dev() + ", which fills it"
	}
	return "dynaflux backfill " + id + " -region " + r.region + h.flags()
}

// stale reports whether version id is live although r's catalog no longer makes it current,
// and retire would take it: its view's current version is live too, or the view has none.
func (r *regionStatus) stale(id string) bool {
	if vv, defined := r.cat.Version(id); !defined || vv.State != catalog.StateRetained || r.state(id).State != meta.Live {
		return false
	}
	view, _, _ := strings.Cut(id, "@v")
	return r.currentLive(view)
}

// currentLive reports whether d's catalog makes no version of view current, or makes one
// that is live, as retire requires of a region.
func (d *deployment) currentLive(view string) bool {
	if d.cat == nil {
		return false
	}
	n := currentNumber(d.cat, view)
	return n == 0 || d.state(catalog.VersionID(view, n)).State == meta.Live
}

// retire returns the command that retires id, stale in r: in the regions that status read,
// when retire would take it in each, and else in r alone, where a retire leaves
// dynaflux.lock as it is.
func (r *regionStatus) retire(f *fleet, id string, h hintFlags) string {
	view, _, _ := strings.Cut(id, "@v")
	regions := h.regions
	if slices.ContainsFunc(f.regions, func(d *deployment) bool { return !d.notDeployed && !d.currentLive(view) }) {
		regions = []string{r.region}
	}
	return "dynaflux retire " + id + h.in(regions)
}

func prefixFlag(p optionalString) string {
	if !p.set {
		return ""
	}
	return " -prefix " + cmp.Or(p.value, `""`)
}

// hintFlags are the flags that chose a status run's environment, as given. Its hints repeat
// them, so that a command pasted from one reaches the same tables: after status -endpoint,
// DynamoDB Local's rather than AWS's.
type hintFlags struct {
	endpoint string
	prefix   optionalString
	regions  repeated
}

// flags returns -endpoint and -prefix as given, and with -endpoint each -region, which picks
// DynamoDB Local's database there. On AWS a hint names the regions it acts in.
func (h hintFlags) flags() string {
	if h.endpoint == "" {
		return prefixFlag(h.prefix)
	}
	return h.in(h.regions)
}

// in returns a -region flag for each of regions, then -endpoint and -prefix as given.
func (h hintFlags) in(regions []string) string {
	var b strings.Builder
	for _, r := range regions {
		b.WriteString(" -region " + r)
	}
	if h.endpoint != "" {
		b.WriteString(" -endpoint " + h.endpoint)
	}
	return b.String() + prefixFlag(h.prefix)
}

// dev returns the command that fills view versions on DynamoDB Local, where backfill
// doesn't run.
func (h hintFlags) dev() string { return "dynaflux dev -endpoint " + h.endpoint }

// noOrigin is DF7218 for stream, whose table, born at born, is less than 23 hours old and has
// no origin in region, which archives it with events: all. st says why backfill -all recorded
// none, when it tried; flags are those that chose the environment, but -region.
func noOrigin(region, stream string, born time.Time, st backfill.OriginStatus, flags string) finding {
	msg := fmt.Sprintf("the table of stream %s is less than 23 hours old and has no origin in %s, which archives it with events: all", stream, region)
	run := fmt.Sprintf("run dynaflux backfill -all -region %s%s", region, flags)
	before := fmt.Sprintf("before %s and before the stream's first event; otherwise the archive of it in %s is never complete",
		timeText(born.Add(backfill.FreshAge)), region)
	hint := run + " " + before
	switch st.State {
	case backfill.OriginTooLate:
		msg += "; it holds events already, so the archive of it there is never complete"
		hint = "backfills there read the table; to keep a new stream's whole history, run dynaflux backfill -all after the deploy that adds it, before its first event"
	case backfill.OriginWaiting:
		hint = fmt.Sprintf("a deploy changed its archive mode: %s again after %s, %s", run, timeText(st.Until), before)
	case backfill.OriginPending:
		hint = fmt.Sprintf("a deploy in %s is under way: %s again once it registered its catalog, %s", region, run, before)
	case backfill.OriginRecorded, backfill.OriginHeld:
	}
	return regionFinding(region, diag.SeverityWarning, "DF7218", msg, hint)
}

// archiveFindings returns what needs an action in the history of stream s in the region:
// DF7218 when its table is less than 23 hours old and has no origin; DF7204 when its archive
// isn't complete, for the versions activating from it; and DF7204 for each gap suspected in
// the views applied live, for the versions activated before it that no finding named yet.
// DF7219 already names the versions whose jobs failed.
func (r *regionStatus) archiveFindings(s streamStatus, h hintFlags, now time.Time) []finding {
	var fs []finding
	named := map[string]bool{}
	for _, j := range r.jobs {
		named[j.Version] = j.State == meta.JobFailed
	}
	if a := s.check; a != nil && !a.unread {
		if a.noOrigin && !a.born.IsZero() && now.Sub(a.born) < backfill.FreshAge {
			fs = append(fs, noOrigin(r.region, s.name, a.born, backfill.OriginStatus{}, h.flags()))
		}
		if v := a.verdict; !v.Complete {
			ids := r.affected(s.name, named, func(vs meta.ViewState) bool { return vs.State == meta.Activating && vs.Split == meta.SplitFence })
			if len(ids) > 0 {
				fs = append(fs, regionFinding(r.region, diag.SeverityWarning, "DF7204",
					fmt.Sprintf("the archive of stream %s isn't complete: %s; %s can't go live from it", s.name, v.Reason, strings.Join(ids, ", ")),
					h.rebuild(ids)))
			}
		}
	}
	for _, v := range s.suspects {
		ids := r.affected(s.name, named, func(vs meta.ViewState) bool { return v.At.IsZero() || vs.ActivatedAt.Before(v.At) })
		if len(ids) > 0 {
			fs = append(fs, regionFinding(r.region, diag.SeverityWarning, "DF7204",
				fmt.Sprintf("a gap is suspected in stream %s: %s; %s may miss its records", s.name, v.Reason, strings.Join(ids, ", ")),
				h.rebuild(ids)))
		}
	}
	return fs
}

// affected returns the activated versions of stream, neither retired nor broken, that match
// and that named doesn't hold, and adds them to it.
func (r *regionStatus) affected(stream string, named map[string]bool, match func(meta.ViewState) bool) []string {
	var ids []string
	for _, id := range r.versionIDs() {
		vs := r.state(id)
		if vs.Stream != stream || vs.State == meta.Retired || vs.State == meta.Broken || vs.ActivatedAt.IsZero() || named[id] || !match(vs) {
			continue
		}
		named[id] = true
		ids = append(ids, id)
	}
	return ids
}

func jobError(j meta.Job) string {
	if j.Error == nil {
		return ""
	}
	return fmt.Sprintf("; its last error, in phase %s at %s: %s", j.Error.Phase, timeText(j.Error.At), j.Error.Message)
}

// broken is DF7201 for the broken version vs, which the catalog defines as vv when defined.
func (r *regionStatus) broken(f *fleet, vs meta.ViewState, vv *catalog.ViewVersion, defined bool, h hintFlags) finding {
	fl := vs.Failure
	event := "event " + fl.EventID
	if fl.Unit != "" {
		event = fmt.Sprintf("the record of unit %s at sequence number %s", fl.Unit, fl.Seq)
		if fl.EventID != "" {
			event += ", event " + fl.EventID + ","
		}
	}
	what := "failed to evaluate"
	if fl.Cause == meta.CauseSplit {
		what = "can't be placed by the time split"
	}
	view, _, _ := strings.Cut(vs.Version, "@")
	fill := ", deploy regional.yaml and run dynaflux backfill " + view + h.flags()
	if h.endpoint != "" {
		fill = " and restart " + h.dev() + ", which fills the new version"
	}
	hint := "fix " + view + ", then run dynaflux generate" + fill
	switch {
	case defined && vv.State == catalog.StateRetained:
		hint = "retire it with dynaflux retire " + vs.Version + h.flags()
	case fl.Cause == meta.CauseSplit:
		hint = "run dynaflux generate -rebuild " + view + fill
	}
	// DynamoDB Local has no archive.
	if sql := r.athena(f, vs.Stream, fl); sql != "" && h.endpoint == "" {
		hint += "; the event in Athena: " + sql
	}
	return regionFinding(r.region, diag.SeverityError, "DF7201",
		fmt.Sprintf("%s broke at %s: %s %s: %s", vs.Version, timeText(fl.At), event, what, fl.Error), hint)
}

// athena returns the Athena SQL that finds a failure's event in the region's archive, or ""
// when the region doesn't archive the stream.
func (r *regionStatus) athena(f *fleet, stream string, fl *meta.Failure) string {
	s, ok := r.cat.Stream(stream)
	if !ok || s.Archive == nil || !slices.Contains(s.Archive.Regions, r.region) {
		return ""
	}
	db := athenaName(f.naming.ArchiveDatabase())
	if fl.EventID != "" {
		return fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %s", db, athenaName(catalog.DedupView(catalog.IcebergTable(stream))),
			athenaName(catalog.ColumnEventID), sqlString(fl.EventID))
	}
	return fmt.Sprintf(`SELECT * FROM %s.%s WHERE "_shard" = %s AND "_seq" = %s`, db, athenaName(catalog.IcebergTable(stream)),
		sqlString(fl.Unit), sqlString(fl.Seq))
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// blocked is DF7209 for the version vs that r's projectors don't know.
func (r *regionStatus) blocked(vs meta.ViewState, now time.Time, h hintFlags) finding {
	loss := "records are lost 24 hours after they were written"
	if s := r.stream(vs.Stream); s != nil && s.lag != nil {
		loss = "records are lost from " + timeText(now.Add(streamRetention-*s.lag))
	}
	hint := "deploy regional.yaml in " + r.region + " from a build whose catalog defines " + vs.Version
	if h.endpoint != "" {
		hint = "restart " + h.dev() + " from a dynaflux.lock that defines " + vs.Version
	}
	return regionFinding(r.region, diag.SeverityError, "DF7209",
		fmt.Sprintf("the projector of stream %s fails every batch: %s is %s, and the deployed catalog neither defines nor retires it; "+
			"%s to every view and the archive", vs.Stream, vs.Version, vs.State, loss), hint)
}

// streamFindings returns what needs an action in the delivery of stream s. h is the flags
// that chose the environment, for the hints.
func (r *regionStatus) streamFindings(f *fleet, s streamStatus, h hintFlags) []finding {
	var fs []finding
	add := func(sev diag.Severity, code, msg, hint string) {
		fs = append(fs, regionFinding(r.region, sev, code, msg, hint))
	}
	redeploy := "deploy regional.yaml in " + r.region + " again"
	mp, ok := s.projector(f.naming)
	switch {
	case !s.mapped:
	case s.arn == "":
		add(diag.SeverityError, "DF7208", "the table of stream "+s.name+" has no change stream, so nothing applies its events", redeploy)
	case !ok:
		add(diag.SeverityError, "DF7208", "stream "+s.name+" has no event source mapping to its projector, so nothing applies its events", redeploy)
	case !mp.Delivering():
		add(diag.SeverityError, "DF7208",
			fmt.Sprintf("the event source mapping %s of stream %s is %s, so nothing applies its events", mp.UUID, s.name, mp.State), redeploy)
	case strings.HasPrefix(mp.LastResult, "PROBLEM"):
		add(diag.SeverityError, "DF7208", fmt.Sprintf("the event source mapping %s of stream %s reports %s", mp.UUID, s.name, mp.LastResult),
			fmt.Sprintf("read why %s fails with aws logs tail %s --since 1h --region %s", mp.Function, catalog.FunctionLogGroup(mp.Function), r.region))
	}
	for _, m := range s.mappings {
		if !isProjector(f.naming, s.name, m.Function) {
			add(diag.SeverityWarning, "DF7206",
				fmt.Sprintf("function %s also reads stream %s, with event source mapping %s; a shard serves at most two readers", m.Function, s.name, m.UUID),
				fmt.Sprintf("unless it is needed, aws lambda delete-event-source-mapping --uuid %s --region %s", m.UUID, r.region))
		}
	}
	if s.lag != nil && *s.lag >= lagWarning {
		add(diag.SeverityWarning, "DF7205", fmt.Sprintf("the projector of stream %s is %s behind", s.name, durationText(*s.lag)),
			fmt.Sprintf("read its log with aws logs tail %s --since 1h --region %s", catalog.FunctionLogGroup(mp.Function), r.region))
	}
	if s.freshness != nil && *s.freshness >= staleArchive {
		add(diag.SeverityWarning, "DF7210", fmt.Sprintf("the archive of stream %s is %s behind", s.name, durationText(*s.freshness)),
			"read the log group "+f.naming.ArchiveLogGroup())
	}
	if s.failed > 0 {
		add(diag.SeverityWarning, "DF7210",
			fmt.Sprintf("Firehose couldn't write %d rows of stream %s to the archive in the last 24 hours", s.failed, s.name),
			fmt.Sprintf("the rows are under s3://%s/errors/firehose/%s/", f.naming.Bucket(f.account, r.region), s.name))
	}
	for _, g := range s.gaps {
		if missing := r.missing(s.name, g); len(missing) > 0 {
			add(diag.SeverityError, "DF7203",
				fmt.Sprintf("Lambda discarded %d records of stream %s: unit %s, sequence numbers %s to %s, which arrived from %s to %s; %s miss them",
					g.Records, s.name, g.Shard, g.FirstSeq, g.LastSeq, timeText(g.FirstArrival), timeText(g.LastArrival), strings.Join(missing, ", ")),
				h.rebuild(missing)+fmt.Sprintf("; the records are in s3://%s/%s", f.naming.Bucket(f.account, r.region), g.Key))
		}
	}
	return fs
}

// rebuild says how to fill the views of versions again: a rebuild of each, a deploy, and a
// backfill, or on DynamoDB Local a run of dev.
func (h hintFlags) rebuild(versions []string) string {
	var views, rebuilds []string
	for _, id := range versions {
		if view, _, _ := strings.Cut(id, "@"); !slices.Contains(views, view) {
			views = append(views, view)
			rebuilds = append(rebuilds, "dynaflux generate -rebuild "+view)
		}
	}
	if h.endpoint != "" {
		return fmt.Sprintf("run %s, then restart %s, which fills the new versions", strings.Join(rebuilds, ", "), h.dev())
	}
	return fmt.Sprintf("run %s, deploy regional.yaml in every region, and run dynaflux backfill %s%s",
		strings.Join(rebuilds, ", "), strings.Join(views, " "), h.flags())
}

// missing returns the versions of stream that miss the gap's records: those neither retired
// nor broken that were activated before the gap's last record arrived. A version activated
// later reads the records from the stream table.
func (r *regionStatus) missing(stream string, g deployed.Gap) []string {
	var ids []string
	for _, id := range r.versionIDs() {
		vs := r.state(id)
		if vs.Stream == stream && vs.State != meta.Retired && vs.State != meta.Broken && !vs.ActivatedAt.IsZero() && vs.ActivatedAt.Before(g.LastArrival) {
			ids = append(ids, id)
		}
	}
	return ids
}

func timeText(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func durationText(d time.Duration) string { return d.Round(time.Second).String() }
