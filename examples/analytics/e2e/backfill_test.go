// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/validate"
)

// Scenarios 24 and 36 change the deployed store as an operator would: they add backfillSQL to
// the project that scripts/e2e/deploy.sh kept in E2E_WORK, and generate, build and deploy it
// there with your credentials, as deploy.sh does. They run its dynaflux build with the
// credentials of E2E_OPERATOR_ROLE, which has the operator grants of docs/spec/infra.md §9.8.
const (
	workVar     = "E2E_WORK"
	operatorVar = "E2E_OPERATOR_ROLE"
	backfillSQL = "testdata/backfill.sql"
)

// backfillSQL's views, and its queries, which read every column of them.
const (
	countView   = "e2e_page_count"  // additive: a table source splits it by time
	latestView  = "e2e_page_latest" // idempotent: a table source needs no split
	countQuery  = "E2EPageCount"
	latestQuery = "E2EPageLatest"
)

const (
	// preload is how many page views scenario 24 appends in each region before its backfill,
	// so that each job applies for minutes at backfillRate.
	preload      = 3000
	backfillRate = 20
	// Traffic appends trafficBatch page views in each region every trafficEvery.
	trafficBatch = 5
	trafficEvery = 5 * time.Second
	// archiveAppends is how long scenario 36 appends before it rebuilds the views.
	archiveAppends = time.Hour
	// backfillPatience is how long a job may take to make its version live: a table source
	// splits by time about 30 minutes after the activation, and an archive source waits for
	// Firehose.
	backfillPatience = time.Hour
)

// Scenario 24: backfills from the stream tables under traffic. While appends go on in every
// region, backfillSQL's views are deployed and filled at 20 records a second from the tables.
// In the home region, the idempotent version's job is cancelled during apply, then resumed.
// The backfill role is denied writes to the additive version's table for a minute, which fails
// no execution, then until the execution fails; the job is resumed with the command that
// status gives, then sped up with -rate.
//
//nolint:thelper // a scenario: its failures point into it
func testBackfillUnderTraffic(t *testing.T, h *harness) {
	k := h.keptProject(t)
	if !k.addViews(t) {
		t.Fatalf("%s's views are in %s already: scenario 24 runs once per store", backfillSQL, k.dir)
	}
	def, cat := k.catalog(t)
	count, latest := currentVersion(t, def, countView), currentVersion(t, def, latestView)
	b := newBackfillRun(t, h, cat, "s24")
	for _, reg := range h.regions {
		for range preload / trafficBatch {
			if err := b.append(t.Context(), reg, trafficBatch, b.all); err != nil {
				t.Fatal(err)
			}
		}
	}
	stop := b.traffic(b.all)
	k.deploy(t)
	k.mustDynaflux(t, "backfill", "-all", "-yes", "-rate", strconv.Itoa(backfillRate), "-source", "table", "-prefix", k.prefix)
	home := h.regions[0]
	cancelApply(t, h, k, home, latest)
	denyApply(t, h, k, home, count)
	for _, reg := range h.regions {
		awaitLive(t, h, reg, count, meta.SplitTime)
		awaitLive(t, h, reg, latest, meta.SplitNone)
	}
	stop()
	for _, reg := range h.regions {
		b.converged(reg, cat, b.all, countQuery, latestQuery)
	}
}

// cancelApply cancels the job of version in reg during apply, checks that the version stays
// activating, and resumes the job.
func cancelApply(t *testing.T, h *harness, k *keptProject, reg *region, version string) {
	t.Helper()
	awaitJob(t, h, reg, version, "in apply", func(j meta.Job) bool { return j.Phase == meta.PhaseApply })
	k.mustDynaflux(t, "backfill", "-cancel", "-region", reg.name, "-prefix", k.prefix, version)
	checkCancelled(t, reg, version)
	k.mustDynaflux(t, "backfill", "-region", reg.name, "-prefix", k.prefix, version)
}

// checkCancelled checks that the job of version in reg is cancelled, and its version
// activating.
func checkCancelled(t *testing.T, reg *region, version string) {
	t.Helper()
	j, _, err := reg.meta.Job(t.Context(), version)
	if err != nil {
		t.Fatal(err)
	}
	states, err := reg.meta.ViewStates(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if s := states[version].State; j.State != meta.JobCancelled || s != meta.Activating {
		t.Errorf("%s: after -cancel, the job of %s is %q and the version %q; want cancelled and activating", reg.name, version, j.State, s)
	}
}

// denyApply denies the backfill role in reg writes to version's table during apply: for a
// minute, after which the job goes on in the same execution, then until the execution fails.
// It resumes the job with the command of status's DF7216, and sets its rate to 200.
func denyApply(t *testing.T, h *harness, k *keptProject, reg *region, version string) {
	t.Helper()
	c := h.aws.clients[reg]
	role := functionRole(t, h, reg, reg.naming.BackfillFunction())
	t.Cleanup(func() {
		if err := removeDeny(context.WithoutCancel(t.Context()), c.iam, role); err != nil {
			t.Error(err)
		}
	})
	view, number := splitVersion(t, version)
	table := "arn:aws:dynamodb:" + reg.name + ":" + h.aws.account + ":table/" + reg.naming.ViewTable(view, number)
	awaitJob(t, h, reg, version, "in apply", func(j meta.Job) bool { return j.Phase == meta.PhaseApply })
	deny(t, c.iam, role, "dynamodb:PutItem", table)
	if err := sleep(t.Context(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := removeDeny(t.Context(), c.iam, role); err != nil {
		t.Fatal(err)
	}
	j, _, err := reg.meta.Job(t.Context(), version)
	if err != nil {
		t.Fatal(err)
	}
	awaitJob(t, h, reg, version, "applying again", func(n meta.Job) bool { return n.Applied > j.Applied })
	if f, ok := k.status(t).finding("DF7216", reg.name, version); ok {
		t.Errorf("%s: the 1-minute deny failed the execution: %s", reg.name, f.Message)
	}
	since := time.Now()
	deny(t, c.iam, role, "dynamodb:PutItem", table)
	var hint string
	h.await(t, backfillPatience/2, reg.name+": DF7216 for "+version, func(context.Context) error {
		f, ok := k.status(t).finding("DF7216", reg.name, version)
		if !ok {
			return errors.New("no DF7216 yet")
		}
		hint = f.Hint
		return nil
	})
	alarm := reg.naming.Base() + "-backfill-failed"
	h.await(t, h.patience, reg.name+": alarm "+alarm, func(ctx context.Context) error {
		return h.aws.alarmFired(ctx, reg, alarm, since)
	})
	if err = removeDeny(t.Context(), c.iam, role); err != nil {
		t.Fatal(err)
	}
	resume, ok := strings.CutPrefix(hint, "resume it with dynaflux ")
	if !ok {
		t.Fatalf("%s: DF7216's hint is %q; want the command that resumes the job", reg.name, hint)
	}
	k.mustDynaflux(t, strings.Fields(resume)...)
	k.mustDynaflux(t, "backfill", "-rate", "200", "-prefix", k.prefix, version)
	h.await(t, h.patience, reg.name+": rate 200 in status", func(context.Context) error {
		jobs := k.status(t).region(reg.name).Jobs
		i := slices.IndexFunc(jobs, func(j jobStatus) bool { return j.Version == version })
		switch {
		case i < 0:
			return errors.New("no job")
		case jobs[i].Rate != 200:
			return fmt.Errorf("rate %d", jobs[i].Rate)
		}
		return nil
	})
}

// Scenario 36: backfills from the archive. After an hour of appends in every region, and a
// sample of events deleted from the stream table as TTL would, backfillSQL's views are rebuilt
// and filled from each region's archive under traffic, with a -cancel during one of a job's
// Athena queries. Then a hole in the second region's archive makes a later version read the
// table there, while the home region still reads its archive.
//
//nolint:thelper // a scenario: its failures point into it
func testArchiveBackfill(t *testing.T, h *harness) {
	k := h.keptProject(t)
	if k.addViews(t) {
		k.deploy(t)
	}
	_, cat := k.catalog(t)
	b := newBackfillRun(t, h, cat, "s36")
	home, other := h.regions[0], h.regions[1]
	since := time.Now()
	b.deleteSample(home)
	// The archive jobs read only what Firehose delivered, which they learn from its freshness:
	// without records, a job waits hours. So appends go on until the last job ends.
	stop := b.traffic(b.all, b.kept)
	if err := sleep(t.Context(), archiveAppends); err != nil {
		t.Fatal(err)
	}
	checkComplete(t, k.status(t), h, nil)
	k.generate(t, "-rebuild", countView)
	k.generate(t, "-rebuild", latestView)
	k.deploy(t)
	def, rebuilt := k.catalog(t)
	versions := []string{currentVersion(t, def, countView), currentVersion(t, def, latestView)}
	k.mustDynaflux(t, "backfill", "-all", "-yes", "-prefix", k.prefix)
	cancelQuery(t, h, k, versions)
	for _, reg := range h.regions {
		for _, v := range versions {
			awaitLive(t, h, reg, v, meta.SplitFence)
		}
	}
	checkArchiveJobs(t, h, k, versions, since)

	hole := time.Now()
	k.archiveIn(t, home.name)
	k.archiveIn(t, "")
	checkComplete(t, k.status(t), h, map[string]time.Time{other.name: hole})
	k.generate(t, "-rebuild", latestView)
	k.deploy(t)
	def, last := k.catalog(t)
	latest := currentVersion(t, def, latestView)
	_, stderr, code := k.dynaflux(t, "backfill", "-source", "archive", "-region", other.name, "-prefix", k.prefix, latest)
	if code != 1 || !strings.Contains(stderr, "DF7503") {
		t.Errorf("%s: backfill -source archive exited with %d, want 1 with DF7503:\n%s", other.name, code, stderr)
	}
	k.mustDynaflux(t, "backfill", "-all", "-yes", "-prefix", k.prefix)
	awaitLive(t, h, home, latest, meta.SplitFence)
	awaitLive(t, h, other, latest, meta.SplitNone)
	stop()
	for _, reg := range h.regions {
		b.converged(reg, rebuilt, b.all, countQuery, latestQuery)
	}
	// The table lost the deleted sample, which the archive kept.
	b.converged(home, last, b.all, latestQuery)
	b.converged(other, last, b.kept, latestQuery)
}

// checkComplete checks status's completeness of every stream's archive in each region: since
// an origin, or for a region in holes, not complete since a time after the one it maps to.
func checkComplete(t *testing.T, doc statusDoc, h *harness, holes map[string]time.Time) {
	t.Helper()
	for _, reg := range h.regions {
		streams := doc.region(reg.name).Streams
		for _, st := range h.p.def.Streams {
			i := slices.IndexFunc(streams, func(s streamStatus) bool { return s.Stream == st.Name })
			if i < 0 || streams[i].Complete == nil {
				t.Errorf("%s: status shows no archive completeness of %s", reg.name, st.Name)
				continue
			}
			c := streams[i].Complete
			hole, holed := holes[reg.name]
			switch {
			case !holed && (!c.Complete || c.Since.IsZero()):
				t.Errorf("%s: the archive of %s is %+v, want complete since an origin", reg.name, st.Name, *c)
			case holed && (c.Complete || c.At.Before(hole)):
				t.Errorf("%s: the archive of %s is %+v, want not complete since %s", reg.name, st.Name, *c, hole.UTC().Format(time.RFC3339))
			}
		}
	}
}

// cancelQuery cancels a job of versions while it runs an Athena query: the listing of the
// archive's partitions or a slice's export. It checks that -cancel stopped the query and left
// the version activating, and resumes the job, which takes a new try. A -cancel that came
// after the query ended resumes the job and waits for another query.
func cancelQuery(t *testing.T, h *harness, k *keptProject, versions []string) {
	t.Helper()
	for deadline := time.Now().Add(backfillPatience); time.Now().Before(deadline); {
		reg, j, query, ended := runningQuery(t, h, versions)
		switch {
		case ended:
			t.Fatal("every job read the archive before a -cancel stopped one of its queries: run the scenario again on a new run")
		case reg == nil:
			if err := sleep(t.Context(), time.Second); err != nil {
				t.Fatal(err)
			}
			continue
		}
		k.mustDynaflux(t, "backfill", "-cancel", "-region", reg.name, "-prefix", k.prefix, j.Version)
		checkCancelled(t, reg, j.Version)
		state := queryEnd(t, h, reg, query)
		k.mustDynaflux(t, "backfill", "-region", reg.name, "-prefix", k.prefix, j.Version)
		if state == athenatypes.QueryExecutionStateCancelled {
			awaitJob(t, h, reg, j.Version, "on a new try", func(n meta.Job) bool { return n.Archive.Tries > j.Archive.Tries })
			return
		}
		t.Logf("%s: query %s of %s %s before -cancel stopped it", reg.name, query, j.Version, state)
	}
	t.Fatal("no job of the rebuilt versions ran an Athena query within the patience")
}

// runningQuery returns a job of versions that runs an Athena query, with the query, or a nil
// region when none does. ended reports that every job ended.
func runningQuery(t *testing.T, h *harness, versions []string) (*region, meta.Job, string, bool) {
	t.Helper()
	ended := true
	for _, reg := range h.regions {
		for _, v := range versions {
			j, ok, err := reg.meta.Job(t.Context(), v)
			if err != nil {
				t.Fatal(err)
			}
			if ok && j.State.Ended() {
				continue
			}
			ended = false
			a := j.Archive
			switch {
			case !ok || j.State != meta.JobRunning || j.Phase != meta.PhaseStage:
			case a.Plan == "" && a.PlanQuery != "":
				return reg, j, a.PlanQuery, false
			case a.Plan != "" && a.Export.ID != "" && a.Query == "":
				return reg, j, a.Export.ID, false
			}
		}
	}
	return nil, meta.Job{}, "", ended
}

// queryEnd waits until an Athena query ends, and returns its state.
func queryEnd(t *testing.T, h *harness, reg *region, id string) athenatypes.QueryExecutionState {
	t.Helper()
	var state athenatypes.QueryExecutionState
	h.await(t, h.patience, reg.name+": the end of query "+id, func(ctx context.Context) error {
		q, err := h.aws.clients[reg].athena.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: &id})
		if err != nil {
			return err
		}
		switch state = q.QueryExecution.Status.State; state {
		case athenatypes.QueryExecutionStateSucceeded, athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			return nil
		case athenatypes.QueryExecutionStateQueued, athenatypes.QueryExecutionStateRunning:
		}
		return fmt.Errorf("state %s", state)
	})
	return state
}

// checkArchiveJobs checks the jobs of versions, which read the archive in every region: each
// scanned some bytes, which it logs, and skipped no line of the error prefix as foreign; and
// the backfill function logged no access denial since a time.
func checkArchiveJobs(t *testing.T, h *harness, k *keptProject, versions []string, since time.Time) {
	t.Helper()
	doc := k.status(t)
	for _, reg := range h.regions {
		for _, v := range versions {
			i := slices.IndexFunc(doc.region(reg.name).Jobs, func(j jobStatus) bool { return j.Version == v })
			if i < 0 || doc.region(reg.name).Jobs[i].Scanned == 0 {
				t.Errorf("%s: status shows no bytes scanned by the job of %s", reg.name, v)
			} else {
				t.Logf("%s: the job of %s scanned %d bytes (record it in the results)", reg.name, v, doc.region(reg.name).Jobs[i].Scanned)
			}
			j, _, err := reg.meta.Job(t.Context(), v)
			if err != nil {
				t.Fatal(err)
			}
			if j.Archive.Errors.Foreign != 0 {
				t.Errorf("%s: the job of %s skipped %d foreign lines of the error prefix", reg.name, v, j.Archive.Errors.Foreign)
			}
		}
		group := "/aws/lambda/" + reg.naming.BackfillFunction()
		msgs, err := h.aws.logLines(t.Context(), reg, group, accessDenied, since)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			t.Errorf("%s: %s: %s", reg.name, group, m)
		}
	}
}

// awaitJob waits until the job of version in reg satisfies done.
func awaitJob(t *testing.T, h *harness, reg *region, version, what string, done func(meta.Job) bool) {
	t.Helper()
	h.await(t, backfillPatience, reg.name+": the job of "+version+" "+what, func(ctx context.Context) error {
		j, ok, err := reg.meta.Job(ctx, version)
		if err != nil || (ok && done(j)) {
			return err
		}
		return fmt.Errorf("%s in phase %s, %d applied", j.State, j.Phase, j.Applied)
	})
}

// awaitLive waits until version is live in reg, and checks its split. A job that ended
// broken or failed fails the test at once.
func awaitLive(t *testing.T, h *harness, reg *region, version string, split meta.Split) {
	t.Helper()
	var s meta.ViewState
	h.await(t, backfillPatience, reg.name+": "+version+" live", func(ctx context.Context) error {
		states, err := reg.meta.ViewStates(ctx)
		if err != nil {
			return err
		}
		if s = states[version]; s.State == meta.Live {
			return nil
		}
		j, ok, err := reg.meta.Job(ctx, version)
		if ok && (j.State == meta.JobBroken || j.State == meta.JobFailed) {
			t.Fatalf("%s: the job of %s is %s: %+v", reg.name, version, j.State, j.Error)
		}
		return cmp.Or(err, fmt.Errorf("%q", s.State))
	})
	if s.Split != split {
		t.Errorf("%s: %s went live with split %q, want %q", reg.name, version, s.Split, split)
	}
}

// currentVersion returns the ID of the version of view that def makes current.
func currentVersion(t *testing.T, def *catalog.Catalog, view string) string {
	t.Helper()
	v, ok := def.View(view)
	if !ok {
		t.Fatalf("the kept project has no view %s", view)
	}
	vv, ok := v.Current()
	if !ok {
		t.Fatalf("view %s has no current version", view)
	}
	return catalog.VersionID(view, vv.Number)
}

// splitVersion splits a version ID into its view and its number.
func splitVersion(t *testing.T, id string) (string, int) {
	t.Helper()
	view, number, _ := strings.Cut(id, "@v")
	n, err := strconv.Atoi(number)
	if err != nil {
		t.Fatalf("version %s: %v", id, err)
	}
	return view, n
}

// keptProject is the e2e store's project in E2E_WORK.
type keptProject struct {
	dir      string
	prefix   string
	stack    string // the regional stack
	regions  []string
	operator aws.CredentialsProvider
}

func (h *harness) keptProject(t *testing.T) *keptProject {
	t.Helper()
	dir, role := keptProjectVars(t, h.aws.account)
	home := h.regions[0]
	k := &keptProject{dir: dir, prefix: home.naming.Prefix, stack: home.naming.Base() + "-regional"}
	for _, reg := range h.regions {
		k.regions = append(k.regions, reg.name)
	}
	k.operator = h.aws.assume(home.cfg, role, operatorVar, nil)
	return k
}

// keptProjectVars returns E2E_WORK and E2E_OPERATOR_ROLE, failing unless the first holds the
// project that deploy.sh kept and the second names a role in account. TestE2E checks them
// before its first scenario, since the scenarios that read them run hours later.
func keptProjectVars(t *testing.T, account string) (string, string) {
	t.Helper()
	dir := os.Getenv(workVar)
	if _, err := os.Stat(filepath.Join(dir, "bin", "dynaflux")); dir == "" || err != nil {
		t.Fatalf("%s is %q; set it to the directory where scripts/e2e/deploy.sh kept the project, as make e2e-aws does", workVar, dir)
	}
	role := os.Getenv(operatorVar)
	if !strings.HasPrefix(role, "arn:aws:iam::"+account+":role/") {
		t.Fatalf("%s is %q; set it to the ARN of a role in E2E_ACCOUNT that you can assume, with the operator grants of docs/spec/infra.md §9.8 (docs/runbooks/e2e.md §1)",
			operatorVar, role)
	}
	return dir, role
}

// addViews copies backfillSQL into the project and generates it, unless an earlier scenario
// did. It reports whether it did.
func (k *keptProject) addViews(t *testing.T) bool {
	t.Helper()
	dst := filepath.Join(k.dir, "dynaflux", filepath.Base(backfillSQL))
	if _, err := os.Stat(dst); err == nil {
		return false
	}
	data, err := os.ReadFile(backfillSQL)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatal(err)
	}
	k.generate(t)
	return true
}

// archiveIn sets archive.regions to one region and generates, or, with region empty,
// restores the example's dynaflux.yaml; then it deploys. The example archives in every
// region.
func (k *keptProject) archiveIn(t *testing.T, region string) {
	t.Helper()
	name := filepath.Join(k.dir, "dynaflux.yaml")
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	const section = "\narchive:\n  regions: "
	data, _, _ = bytes.Cut(data, []byte(section))
	if region != "" {
		data = append(data, section+"["+region+"]\n"...)
	}
	if err = os.WriteFile(name, data, 0o600); err != nil {
		t.Fatal(err)
	}
	k.generate(t)
	k.deploy(t)
}

// generate runs dynaflux generate in the project.
func (k *keptProject) generate(t *testing.T, args ...string) {
	t.Helper()
	k.must(t, nil, filepath.Join("bin", "dynaflux"), append([]string{"generate"}, args...)...)
}

// deploy builds what generate wrote and deploys regional.yaml in every region.
func (k *keptProject) deploy(t *testing.T) {
	t.Helper()
	k.must(t, []string{"CGO_ENABLED=0", "GOFLAGS=-tags=lambda.norpc"}, "sam", "build", "-t", "infra/dynaflux/regional.yaml")
	for _, region := range k.regions {
		k.must(t, nil, "sam", "deploy", "--stack-name", k.stack, "--region", region,
			"--capabilities", "CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND", "--resolve-s3",
			"--parameter-overrides", "Prefix="+k.prefix, "--no-fail-on-empty-changeset")
	}
}

// catalog loads the project as generate last wrote it.
func (k *keptProject) catalog(t *testing.T) (*catalog.Catalog, *dynaflux.Catalog) {
	t.Helper()
	_, def, ds := validate.Load(os.DirFS(k.dir), k.dir)
	if def == nil {
		var b bytes.Buffer
		if err := diag.Write(&b, ds); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("the project in %s doesn't compile:\n%s", k.dir, b.Bytes())
	}
	data, err := catalog.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	return def, fluxgen.LoadCatalog(string(data))
}

// dynaflux runs the project's dynaflux build as the operator, and returns its stdout, its
// stderr and its exit status.
func (k *keptProject) dynaflux(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	cr, err := k.operator.Retrieve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The default chain takes these variables over a profile.
	env := []string{"AWS_ACCESS_KEY_ID=" + cr.AccessKeyID, "AWS_SECRET_ACCESS_KEY=" + cr.SecretAccessKey, "AWS_SESSION_TOKEN=" + cr.SessionToken}
	return k.run(t, env, filepath.Join("bin", "dynaflux"), args...)
}

// mustDynaflux runs dynaflux as the operator, fails the test unless it exits with 0, and
// returns its stdout.
func (k *keptProject) mustDynaflux(t *testing.T, args ...string) string {
	t.Helper()
	stdout, stderr, code := k.dynaflux(t, args...)
	if code != 0 {
		t.Fatalf("dynaflux %s exited with %d:\n%s", strings.Join(args, " "), code, stderr)
	}
	return stdout
}

// must runs a command in the project with your credentials, and fails the test unless it
// succeeds.
func (k *keptProject) must(t *testing.T, env []string, name string, args ...string) {
	t.Helper()
	if _, stderr, code := k.run(t, env, name, args...); code != 0 {
		t.Fatalf("%s %s exited with %d:\n%s", name, strings.Join(args, " "), code, stderr)
	}
}

// run runs a command in the project, with env added to this process's environment, and
// returns its stdout, its stderr and its exit status. The command's text goes to the log.
func (k *keptProject) run(t *testing.T, env []string, name string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir, cmd.Env = k.dir, append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	t.Logf("%s %s", filepath.Base(name), strings.Join(args, " "))
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return stdout.String(), stderr.String(), 0
	case errors.As(err, &exit):
		return stdout.String(), stderr.String(), exit.ExitCode()
	}
	t.Fatal(err)
	return "", "", 0
}

// status runs dynaflux status -json as the operator. Exit status 1 is fine: scenario 5 broke a
// version.
func (k *keptProject) status(t *testing.T) statusDoc {
	t.Helper()
	stdout, stderr, code := k.dynaflux(t, "status", "-json", "-prefix", k.prefix)
	if code > 1 {
		t.Fatalf("dynaflux status exited with %d:\n%s", code, stderr)
	}
	var doc statusDoc
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("status -json: %v", err)
	}
	return doc
}

// statusDoc is what the scenarios read of status -json (docs/spec/lifecycle.md).
type statusDoc struct {
	Regions  []regionStatus  `json:"regions"`
	Findings []statusFinding `json:"findings"`
}

type regionStatus struct {
	Region  string         `json:"region"`
	Jobs    []jobStatus    `json:"jobs"`
	Streams []streamStatus `json:"streams"`
}

type jobStatus struct {
	Version string `json:"version"`
	Rate    int    `json:"rate"`
	Scanned int64  `json:"scanned_bytes"`
}

type streamStatus struct {
	Stream   string `json:"stream"`
	Complete *struct {
		Complete bool      `json:"complete"`
		Since    time.Time `json:"since"`
		Reason   string    `json:"reason"`
		At       time.Time `json:"at"`
	} `json:"archive_completeness"`
}

type statusFinding struct {
	Regions []string `json:"regions"`
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Hint    string   `json:"hint"`
}

func (d statusDoc) region(name string) regionStatus {
	i := slices.IndexFunc(d.Regions, func(r regionStatus) bool { return r.Region == name })
	if i < 0 {
		return regionStatus{}
	}
	return d.Regions[i]
}

// finding returns the finding of code in region whose message names version.
func (d statusDoc) finding(code, region, version string) (statusFinding, bool) {
	i := slices.IndexFunc(d.Findings, func(f statusFinding) bool {
		return f.Code == code && slices.Contains(f.Regions, region) && strings.Contains(f.Message, version+" ")
	})
	if i < 0 {
		return statusFinding{}, false
	}
	return d.Findings[i], true
}

// backfillRun is the page views of scenario 24 or 36, on pages of its own, and two oracles
// with the catalog of the kept project: all gets every event, and kept every event that the
// stream tables still hold. Every append reaches every region on AWS, so the oracles serve
// every region.
type backfillRun struct {
	t         *testing.T
	h         *harness
	tag       string
	pages     []string
	cat       *dynaflux.Catalog // the oracles'
	all, kept *dynaflux.Store
	n         int // the events appended so far
}

func newBackfillRun(t *testing.T, h *harness, cat *dynaflux.Catalog, name string) *backfillRun {
	t.Helper()
	b := &backfillRun{t: t, h: h, tag: name + "-" + nonce, cat: cat, all: fluxtest.NewStore(t, cat), kept: fluxtest.NewStore(t, cat)}
	for i := range 4 {
		b.pages = append(b.pages, b.tag+"-/p"+strconv.Itoa(i))
	}
	return b
}

// append appends n page views in reg, over the run's pages and 50 users, and to oracles.
func (b *backfillRun) append(ctx context.Context, reg *region, n int, oracles ...*dynaflux.Store) error {
	evs := make([]flux.PageView, n)
	for i := range evs {
		b.n++
		evs[i] = flux.PageView{
			UserID: b.tag + "-u" + strconv.Itoa(b.n%50), Page: b.pages[b.n%len(b.pages)],
			DurationMS: uint32(1 + b.n%1000), EventTime: time.Now().UTC(),
		}
	}
	return b.add(ctx, reg, evs, oracles...)
}

func (b *backfillRun) add(ctx context.Context, reg *region, evs []flux.PageView, oracles ...*dynaflux.Store) error {
	if _, err := reg.client.AppendPageViewBatch(ctx, evs); err != nil {
		return fmt.Errorf("append in %s: %w", reg.name, err)
	}
	for _, o := range oracles {
		if _, err := flux.New(o).AppendPageViewBatch(ctx, evs); err != nil {
			return fmt.Errorf("append to the oracle: %w", err)
		}
	}
	return nil
}

// deleteSample appends a page view of a user of its own to each page in reg, and to the
// oracle of every event, and deletes them from the stream table, as TTL would.
func (b *backfillRun) deleteSample(reg *region) {
	b.t.Helper()
	evs := make([]flux.PageView, len(b.pages))
	for i, page := range b.pages {
		evs[i] = flux.PageView{UserID: b.tag + "-deleted-" + strconv.Itoa(i), Page: page, DurationMS: 7, EventTime: time.Now().UTC()}
	}
	if err := b.add(b.t.Context(), reg, evs, b.all); err != nil {
		b.t.Fatal(err)
	}
	for _, ev := range evs {
		deleteStreamItem(b.t, reg, "page_views", "user_id", ev.UserID)
	}
}

// traffic appends a batch in every region every trafficEvery, to oracles too, until the
// function it returns stops it. The appends use the test's context, so that a stop never cuts
// one short.
func (b *backfillRun) traffic(oracles ...*dynaflux.Store) func() {
	stop, done := make(chan struct{}), make(chan error, 1)
	go func() {
		for {
			for _, reg := range b.h.regions {
				if err := b.append(b.t.Context(), reg, trafficBatch, oracles...); err != nil {
					done <- err
					return
				}
			}
			select {
			case <-stop:
				done <- nil
				return
			case <-time.After(trafficEvery):
			}
		}
	}()
	return func() {
		b.t.Helper()
		close(stop)
		if err := <-done; err != nil {
			b.t.Fatal(err)
		}
	}
}

// converged waits until reg's rows of queries, read with cat, equal oracle's, for the run's
// pages.
func (b *backfillRun) converged(reg *region, cat *dynaflux.Catalog, oracle *dynaflux.Store, queries ...string) {
	b.t.Helper()
	b.h.await(b.t, b.h.patience, reg.name+": "+strings.Join(queries, ", ")+" against the oracle", func(ctx context.Context) error {
		want, err := b.rows(ctx, oracle, b.cat, queries)
		if err != nil {
			return fmt.Errorf("oracle: %w", err)
		}
		got, err := b.rows(ctx, reg.store, cat, queries)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("rows\n%+v\nwant\n%+v", got, want)
		}
		return nil
	})
}

// pageRow is a row of countQuery or latestQuery: each sets its own fields.
type pageRow struct {
	Views, TotalMS uint64
	LastView       time.Time
	Users          uint64
}

// rows returns the rows of each query for each of the run's pages.
func (b *backfillRun) rows(ctx context.Context, s *dynaflux.Store, cat *dynaflux.Catalog, queries []string) (map[string][]pageRow, error) {
	out := map[string][]pageRow{}
	for _, q := range queries {
		fields := func(r *pageRow) []any { return []any{&r.Views, &r.TotalMS} }
		if q == latestQuery {
			fields = func(r *pageRow) []any { return []any{&r.LastView, &r.Users} }
		}
		for _, page := range b.pages {
			rows, err := fluxgen.QueryRows(ctx, s, cat, q, []any{&page}, fields)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", q, page, err)
			}
			out[q+" "+page] = rows
		}
	}
	return out, nil
}
