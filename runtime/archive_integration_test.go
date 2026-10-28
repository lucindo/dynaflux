// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill/awsbackfilltest"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// The versions TestArchiveBackfill fills: site_totals counts views and their users, which
// takes the fenced handoff; page_users is idempotent.
const (
	siteTotals = "site_totals@v1"
	pageUsers  = "page_users@v1"
)

// The archive source end to end on the AWS adapters (ADR 0016, 0017): projector functions
// archive page views through the PutRecordBatch fake; then stream items are deleted, as LOG
// RETENTION would, so only the archive holds their events, one of them in the error prefix
// alone. Jobs of a new additive version and a new idempotent one are filled from the archive
// by Step, through the real awsbackfill adapters against the Athena, S3, CloudWatch,
// Firehose and Lambda fakes, while appends go on: both end live with a row for every event.
// With one failed try, each export takes a new try in a prefix and with a token of its own.
func TestArchiveBackfill(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		fail bool // the first try of each export fails
	}{{"happy path", false}, {"one failed try", true}} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := newArchiveRig(t)
			if tt.fail {
				a.athena.FailExports(func(dest string) bool { return strings.HasSuffix(dest, "/1/") })
			}
			a.archiveOnly()
			if err := a.backfill(); err != nil {
				t.Fatal(err)
			}
			a.view("/after", "after-1", 10)
			a.deliver()
			a.check(tt.fail)
		})
	}
}

// archiveOnly appends views and delivers them, then deletes three from the table, as LOG
// RETENTION would, so that only the archive holds them, and the last only its error prefix.
func (a *archiveRig) archiveOnly() {
	a.t.Helper()
	for i := range 12 {
		a.view(fmt.Sprintf("/p%d", i%4), fmt.Sprintf("u%d", i%5), uint64(100+i))
	}
	gone := []store.Key{a.view("/gone-1", "ghost-1", 50), a.view("/gone-2", "ghost-2", 60), a.view("/gone-3", "ghost-3", 70)}
	a.deliver()
	a.moveToErrors("ghost-3")
	for _, k := range gone {
		a.remove(pageViews, k)
	}
	a.deliver()
}

// backfill fills both versions from the archive, a job each, while views are appended.
func (a *archiveRig) backfill() error {
	versions := []string{siteTotals, pageUsers}
	for _, version := range versions {
		if _, err := backfill.Start(a.t.Context(), a.meta, backfill.Request{
			Version: version, Stream: pageViews, Name: strings.ReplaceAll(version, "@", "-"), Execution: backfill.LocalExecution, Split: meta.SplitFence,
		}); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	errs := make([]error, len(versions))
	for i, version := range versions {
		wg.Go(func() { errs[i] = a.stepUntilDone(version) })
	}
	for i := range 5 {
		a.view("/late", fmt.Sprintf("late-%d", i), 10)
		a.deliver()
		time.Sleep(300 * time.Millisecond)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// archiveRig is TestArchiveBackfill's rig: the local rig, an archive of page_views on the
// fakes, and the views appended.
type archiveRig struct {
	*localRig

	env    *backfill.Env
	s3     *awsbackfilltest.S3
	athena *awsbackfilltest.Athena
	bucket string
	views  map[[2]string]bool // (page, user)
	n      int                // the views appended

	mu    sync.Mutex
	moved []byte // the record that only the error prefix holds
}

// newArchiveRig returns a rig whose page_views is fresh, with every version live but the two
// the test fills, an archive mode of all, and an origin.
func newArchiveRig(t *testing.T) *archiveRig {
	t.Helper()
	r := newLocalRig(t, siteTotals, pageUsers)
	ctx := t.Context()
	data, err := catalog.Marshal(rules.cat.Catalog())
	if err == nil {
		_, err = r.meta.RegisterCatalog(ctx, data, "test")
	}
	if err == nil {
		err = r.meta.ApplyArchiveModes(ctx, nil, map[string]meta.ArchiveMode{pageViews: meta.ArchiveAll}, true, time.Now())
	}
	if err != nil {
		t.Fatal(err)
	}
	a := &archiveRig{localRig: r, s3: awsbackfilltest.NewS3(), bucket: r.naming.Bucket(account, emulatortest.Region), views: map[[2]string]bool{}}
	a.athena = awsbackfilltest.NewAthena(a.s3, a.bucket, r.naming.ArchiveDatabase(), rules.cat.Catalog(), emulatortest.Region, a.records)
	// The fakes and the archive see the time a watermark poll two minutes on would: the fake
	// Firehose holds no record, so DataFreshness is 0 a minute after the cut.
	ahead := func() time.Time { return time.Now().Add(2 * time.Minute) }
	metrics := awsbackfilltest.NewMetrics(ahead)
	metrics.Set("AWS/Firehose", "DeliveryToIceberg.DataFreshness", map[string]string{"DeliveryStreamName": r.naming.ArchiveStream(pageViews)},
		func(time.Time) (float64, bool) { return 0, true })
	arn := r.stream(pageViews).arn
	mappings := awsbackfilltest.NewLambda(awsbackfilltest.Mapping{
		UUID: "0a1b", EventSourceArn: arn, State: "Enabled",
		FunctionArn: "arn:aws:lambda:" + emulatortest.Region + ":" + account + ":function:" + r.naming.ProjectorFunction(),
	})
	cw := cloudwatch.NewFromConfig(fakeConfig(metrics))
	clients := awsbackfill.Clients{
		DynamoDB: r.db, S3: s3.NewFromConfig(fakeConfig(a.s3), func(o *s3.Options) { o.UsePathStyle = true }), CloudWatch: cw, Metrics: cw,
		Athena:   athena.NewFromConfig(fakeConfig(a.athena)),
		Firehose: fh.NewFromConfig(fakeConfig(awsbackfilltest.NewFirehose(r.naming.ArchiveStream(pageViews)))),
		Lambda:   lambda.NewFromConfig(fakeConfig(mappings)),
	}
	reader := awsbackfill.NewArchive(clients, r.naming, rules.cat.Catalog(), a.bucket)
	reader.Now = ahead
	a.env = &backfill.Env{
		Naming: r.naming, Catalog: rules.cat.Catalog(), Hash: catalog.Hash(data), Meta: r.meta, Store: r.b,
		Core:  projector.New(rules.cat.Projector(), r.b, nil, projector.Config{Naming: r.naming, Region: emulatortest.Region}),
		Units: shards{client: dynamodbstreams.NewFromConfig(r.cfg), arn: arn}, Tables: minuteBorn{awsbackfill.NewReplicas(r.cfg)},
		Archive: reader, Witness: reader, Region: emulatortest.Region, Runs: awsbackfill.NewRuns(clients.S3, a.bucket),
		Lag: noLag{}, Bounds: meta.Bounds{Listing: time.Second}, Memory: 1 << 20, Chunk: 3, Log: slog.New(slog.DiscardHandler),
	}
	if st, oerr := backfill.RecordOrigin(ctx, a.env, pageViews); oerr != nil || st.State != backfill.OriginRecorded {
		t.Fatalf("the origin of a fresh stream: %+v, %v", st, oerr)
	}
	return a
}

// minuteBorn reports a stream table born at the start of its minute: DynamoDB Local rounds
// ApproximateCreationDateTime down to the minute (ADR 0009), so the records of the table's
// first minute would arrive before it, as an older table's do.
type minuteBorn struct{ backfill.Tables }

func (m minuteBorn) Born(ctx context.Context, table string) (time.Time, bool, error) {
	born, restored, err := m.Tables.Born(ctx, table)
	return born.Truncate(time.Minute), restored, err
}

// fakeConfig returns an SDK configuration in the rig's region, with fake credentials and a
// loopback endpoint that is never dialed: h answers in process.
func fakeConfig(h http.Handler) aws.Config {
	return aws.Config{
		Region:       emulatortest.Region,
		BaseEndpoint: aws.String("http://127.0.0.1:1"),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake", Source: "test"}, nil
		}),
		HTTPClient:       inProcess{h},
		RetryMaxAttempts: 1,
	}
}

// inProcess is an HTTP client that serves each request with h, opening no socket.
type inProcess struct{ h http.Handler }

func (c inProcess) Do(req *http.Request) (*http.Response, error) {
	if req.Body == nil {
		req.Body = http.NoBody
	}
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

// records is the archive's records of stream, which Athena reads: those the delivery stream
// received, less the one only the error prefix holds.
func (a *archiveRig) records(stream string) [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.DeleteFunc(a.fh.received(a.naming.ArchiveStream(stream)), func(rec []byte) bool { return bytes.Equal(rec, a.moved) })
}

// view appends a view of page by user that took ms, and returns its key.
func (a *archiveRig) view(page, user string, ms uint64) store.Key {
	a.t.Helper()
	a.views[[2]string{page, user}] = true
	a.n++
	return a.pageView(page, user, ms)
}

// deliver delivers the stream's new records to a new function, as a mapping does, until they
// all succeed: a function reads the view states when it starts, so a batch that meets a
// fence it can't account for is delivered again.
func (a *archiveRig) deliver() {
	a.t.Helper()
	shard, recs := a.next(pageViews)
	event := a.event(pageViews, shard, recs...)
	for range 10 {
		if reply, err := a.call(a.function(faults{}), event); reply == success && err == nil {
			return
		}
	}
	a.t.Fatalf("records of %s never delivered", shard)
}

// moveToErrors moves the archive record of user's view to the error prefix, as a record that
// Firehose couldn't write to the table, gzipped, with a line that Dynaflux didn't write.
func (a *archiveRig) moveToErrors(user string) {
	a.t.Helper()
	i := slices.IndexFunc(a.fh.received(a.naming.ArchiveStream(pageViews)), func(rec []byte) bool {
		return bytes.Contains(rec, []byte(`"user_id":"`+user+`"`))
	})
	if i < 0 {
		a.t.Fatalf("no archive record of %s", user)
	}
	a.mu.Lock()
	a.moved = a.fh.received(a.naming.ArchiveStream(pageViews))[i]
	a.mu.Unlock()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	for _, raw := range [][]byte{a.moved, []byte(`{"page":"/probe"}`)} {
		line, err := json.Marshal(map[string]any{
			"attemptsMade": 1, "arrivalTimestamp": time.Now().UnixMilli(), "errorCode": "Iceberg.InvalidRecord",
			"errorMessage": "The record is invalid.", "rawData": base64.StdEncoding.EncodeToString(raw),
		})
		if err != nil {
			a.t.Fatal(err)
		}
		if _, err = zw.Write(append(line, '\n')); err != nil {
			a.t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		a.t.Fatal(err)
	}
	a.s3.Put(a.bucket, archive.ErrorPrefix(pageViews)+"2026/10/28/12/errors-1.gz", b.Bytes())
}

// stepUntilDone steps attempt 1 of version's job until it ends, each step a minute and a half
// long, as the state machine does, waiting a second at most between them.
func (a *archiveRig) stepUntilDone(version string) error {
	for range 300 {
		ctx, cancel := context.WithTimeout(a.t.Context(), 90*time.Second)
		wait, done, err := backfill.Step(ctx, a.env, version, 1, "local")
		cancel()
		if err != nil || done {
			return err
		}
		time.Sleep(min(wait, time.Second))
	}
	return fmt.Errorf("the job of %s never ended", version)
}

var unloadTo = regexp.MustCompile(`\nTO '([^']*)'`)

// check checks that both versions are live with a row for every view, that each job read the
// archive and its error prefix, with a failed try of each export when failed, and left
// nothing staged.
func (a *archiveRig) check(failed bool) {
	a.t.Helper()
	users := map[string]bool{}
	for v := range a.views {
		users[v[1]] = true
		a.checkRow(v[0]+" "+v[1], pageUsers, v[0], v[1])
	}
	a.checkRow(fmt.Sprintf("s1 %d %d", len(users), a.n), siteTotals, "s1") // a row holds its aggregates by name
	var tries int
	for _, version := range []string{siteTotals, pageUsers} {
		j, _, err := a.meta.Job(a.t.Context(), version)
		if err != nil || j.State != meta.JobDone || j.Fresh || j.Applied == 0 || j.Archive.Errors.Foreign != 1 || j.Archive.Scanned == 0 {
			a.t.Errorf("job %s: %+v, %v; want done from the archive, 1 foreign line in the error prefix", version, j, err)
		}
		tries += j.Archive.Tries
	}
	var dests []string
	for _, s := range a.athena.Started() {
		if m := unloadTo.FindStringSubmatch(s); m != nil {
			dests = append(dests, m[1])
		}
	}
	// A try's prefix ends in its number.
	var ends []string
	for _, d := range dests {
		ends = append(ends, d[strings.LastIndex(d[:len(d)-1], "/"):])
	}
	slices.Sort(ends)
	want, wantTries := []string{"/1/", "/1/"}, 0
	if failed {
		want, wantTries = []string{"/1/", "/1/", "/2/", "/2/"}, 2
	}
	if !slices.Equal(ends, want) || tries != wantTries {
		a.t.Errorf("exports to %q after %d failed tries; want tries %q and %d failed", dests, tries, want, wantTries)
	}
	if keys := a.s3.Keys(a.bucket, "backfill/"); len(keys) != 0 {
		a.t.Errorf("staged objects %q are left", keys)
	}
}
