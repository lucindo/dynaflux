// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/localstore"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// devWait bounds each wait for dev: an additive version goes live about 7 seconds after its
// activation.
const devWait = time.Minute

// devStore is testdata/dev.txtar's project with a prefix of its own, whose store dev runs on
// DynamoDB Local. The tables of each of its versions are deleted when the test ends.
type devStore struct {
	t        *testing.T
	dir      string
	prefix   string
	endpoint string
	cfg      aws.Config // a client's with the access key that -endpoint sends
	engine   *engine.Engine
	meta     *meta.Meta
	data     []byte          // the catalog, as last compiled
	cat      *engine.Catalog // data compiled
}

func newDevStore(t *testing.T) *devStore {
	t.Helper()
	dir := writeProject(t, "dev")
	prefix := "d" + strings.ToLower(rand.Text()[:12]) + "-"
	f, err := os.OpenFile(filepath.Join(dir, "dynaflux.yaml"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(f, "prefix: %s\n", prefix)
	if err = errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	cfg := emulatortest.Config(t, nil)
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: cli.LocalAccessKey, SecretAccessKey: "fake"}, nil
	})
	b := dynamo.New(dynamodb.NewFromConfig(cfg))
	s := &devStore{
		t: t, dir: dir, prefix: prefix, endpoint: *cfg.BaseEndpoint, cfg: cfg,
		engine: engine.New(b, engine.Options{Region: emulatortest.Region, Prefix: prefix}),
		meta:   meta.New(b, catalog.Naming{Prefix: prefix, Store: "devshop"}.MetaTable()),
	}
	s.generate()
	return s
}

// generate writes dynaflux.lock as dynaflux generate does, and deletes the tables of the
// project's manifest when the test ends.
func (s *devStore) generate() {
	t := s.t
	t.Helper()
	lockProject(t, s.dir)
	res := compile(t, s.dir)
	var err error
	if s.data, err = catalog.Marshal(res.Catalog); err != nil {
		t.Fatal(err)
	}
	if s.cat, err = engine.Compile(s.data); err != nil {
		t.Fatal(err)
	}
	m, err := infra.Build(res.Config, res.Catalog, infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ts, err := localstore.Tables(m, s.prefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithoutCancel(t.Context()) // t.Context is canceled before cleanups run
	c := dynamodb.NewFromConfig(s.cfg)
	t.Cleanup(func() {
		if _, derr := localstore.Delete(ctx, c, ts); derr != nil {
			t.Error(derr)
		}
	})
}

// head is the line every command run on the store prints first.
func (s *devStore) head() string {
	return "store devshop  prefix " + s.prefix + "  endpoint " + s.endpoint + "  region us-east-1\n"
}

// run runs a command with -endpoint on the store.
func (s *devStore) run(args ...string) result {
	var stdout, stderr strings.Builder
	status := cli.Run(s.t.Context(), slices.Concat(args[:1], []string{"-endpoint", s.endpoint}, args[1:]), nil, &stdout, &stderr, s.dir)
	return result{status, stdout.String(), stderr.String()}
}

// devProcess is dynaflux dev running in the background.
type devProcess struct {
	cancel         context.CancelFunc
	done           chan struct{}
	status         int
	stdout, stderr devBuffer
}

type devBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *devBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *devBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// start runs dev with args until the test stops it, and returns once dev printed the
// application's environment.
func (s *devStore) start(args ...string) *devProcess {
	t := s.t
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	r := &devProcess{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.status = cli.Run(ctx, slices.Concat([]string{"dev", "-endpoint", s.endpoint}, args), nil, &r.stdout, &r.stderr, s.dir)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
	})
	r.waitFor(t, "the application's environment", func() bool { return strings.Contains(r.stdout.String(), catalog.EnvPrefix+"=") })
	return r
}

// stop interrupts dev and returns its exit status and output.
func (r *devProcess) stop(t *testing.T) result {
	t.Helper()
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(devWait):
		t.Fatalf("dev didn't stop when interrupted; it printed\n%s", r.stderr.String())
	}
	return result{r.status, r.stdout.String(), r.stderr.String()}
}

// waitFor waits until ok, failing the test when dev ends first or the wait times out.
func (r *devProcess) waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(devWait)
	for !ok() {
		select {
		case <-r.done:
			t.Fatalf("dev ended with status %d before %s; it printed\n%s", r.status, what, r.stderr.String())
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s after %v; dev printed\n%s", what, devWait, r.stderr.String())
		}
	}
}

// waitLive waits until every view version of the project is live.
func (s *devStore) waitLive(r *devProcess) {
	t := s.t
	t.Helper()
	r.waitFor(t, "every version live", func() bool {
		states, err := s.meta.ViewStates(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range s.cat.Catalog().Views {
			for _, vv := range v.Versions {
				if states[catalog.VersionID(v.Name, vv.Number)].State != meta.Live {
					return false
				}
			}
		}
		return true
	})
}

// states returns each version's state and split.
func (s *devStore) states() map[string]string {
	s.t.Helper()
	states, err := s.meta.ViewStates(s.t.Context())
	if err != nil {
		s.t.Fatal(err)
	}
	out := map[string]string{}
	for id, vs := range states {
		out[id] = string(vs.State) + " " + string(vs.Split)
	}
	return out
}

// devPageView is an event of page_views.
type devPageView struct {
	user, page string
	ms         uint64
	at         string // event_time
}

func (p devPageView) values(t *testing.T) []expr.Value {
	t.Helper()
	return []expr.Value{expr.String(p.user), expr.String(p.page), expr.Uint(p.ms), devTime(t, p.at)}
}

// devTime returns the time s, in RFC 3339, as an event_time.
func devTime(t *testing.T, s string) expr.Value {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return expr.Micros(tm.UnixMicro())
}

// devAppend appends events to stream through e, as an application does.
func devAppend(t *testing.T, e *engine.Engine, cat *engine.Catalog, stream string, events ...[]expr.Value) {
	t.Helper()
	res, err := e.Append(t.Context(), cat, stream, events, engine.AppendMode{})
	for i := range res {
		err = errors.Join(err, res[i].Err)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func firstViews() []devPageView {
	return []devPageView{
		{"u1", "/home", 100, "2026-01-01T10:00:00Z"},
		{"u2", "/home", 200, "2026-01-01T11:00:00Z"},
		{"u1", "/home", 300, "2026-01-02T09:00:00Z"},
		{"u1", "/cart", 50, "2026-01-02T12:00:00Z"},
	}
}

func (s *devStore) appendViews(views []devPageView) {
	s.t.Helper()
	for _, v := range views {
		devAppend(s.t, s.engine, s.cat, "page_views", v.values(s.t))
	}
}

// devLines returns the lines of out that aren't a batch's.
func devLines(out string) []string {
	var keep []string
	for line := range strings.Lines(out) {
		if !strings.HasPrefix(line, "page_views: ") && !strings.HasPrefix(line, "clicks: ") {
			keep = append(keep, strings.TrimSuffix(line, "\n"))
		}
	}
	return keep
}

// An application appends while dev runs, and once dev made each version live, query and
// inspect with -endpoint read its projections.
func TestDev(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	r := s.start()
	s.appendViews(firstViews())
	s.waitLive(r)
	stats := result{0, "day                   views\n2026-01-01T00:00:00Z  2\n2026-01-02T00:00:00Z  1\n", s.head()}
	r.waitFor(t, "every event projected", func() bool { return s.run("query", "PageStats", "-param", "page=/home") == stats })
	if got, want := s.run("query", "PageUsers", "-param", "page=/home"), (result{0, "visitors  longest\n2         300\n", s.head()}); got != want {
		t.Errorf("query PageUsers:\ngot  %+v\nwant %+v", got, want)
	}
	if got, want := s.run("inspect", "page_users", "-key", "page=/cart"), (result{0, "page   visitors  longest\n/cart  1         50\n", s.head()}); got != want {
		t.Errorf("inspect page_users:\ngot  %+v\nwant %+v", got, want)
	}

	got := r.stop(t)
	p := s.prefix
	env := "AWS_ENDPOINT_URL_DYNAMODB=" + s.endpoint + "\nAWS_REGION=us-east-1\nAWS_ACCESS_KEY_ID=dynafluxlocal\nAWS_SECRET_ACCESS_KEY=fake\nDYNAFLUX_PREFIX=" + p + "\n"
	want := []string{
		strings.TrimSuffix(s.head(), "\n"),
		"created table " + p + "devshop-stream-page_views",
		"created table " + p + "devshop-meta",
		"created table " + p + "devshop-view-page_daily-v1",
		"created table " + p + "devshop-view-page_users-v1",
		"registered catalog " + catalog.Hash(s.data)[:12],
		"caught up",
		"page_daily@v1: filling from the stream table, split time",
		"page_users@v1: filling from the stream table, split time",
		"page_daily@v1: live",
		"page_users@v1: live",
	}
	if got.status != 0 || got.stdout != env || !slices.Equal(devLines(got.stderr), want) {
		t.Errorf("dev: status %d, stdout\n%s\nstderr\n%s\nwant status 0, stdout\n%s\nstderr without batches\n%s",
			got.status, got.stdout, got.stderr, env, strings.Join(want, "\n"))
	}
}

// Dev stops at an event that an application appended in another region: its projector
// would apply the event's SYNC views a second time.
func TestDevRefusesAnotherRegion(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	r := s.start()
	other := engine.New(dynamo.New(dynamodb.NewFromConfig(s.cfg)), engine.Options{Region: "us-west-2", Prefix: s.prefix})
	devAppend(t, other, s.cat, "page_views", firstViews()[0].values(t))
	select {
	case <-r.done:
	case <-time.After(devWait):
		t.Fatalf("dev didn't stop at the event; it printed\n%s", r.stderr.String())
	}
	refusal := "an event was appended in region us-west-2, but dynaflux dev runs us-east-1, whose projector would apply its SYNC views again"
	hint := "\n  hint: set AWS_REGION=us-east-1 in the application, as dev prints, then run dynaflux dev -reset: the stream table keeps the event\n"
	if got := r.stderr.String(); r.status != 1 || !strings.Contains(got, "\npage_views: 1 record; failed: "+refusal+"\n") || !strings.HasSuffix(got, refusal+hint) {
		t.Errorf("dev: status %d, stderr\n%s\nwant status 1, the batch's refusal and the hint\n%s", r.status, got, refusal+hint)
	}
}

// devQuery is a query and its String parameter.
type devQuery struct{ name, param string }

// devResults returns what each query returns from e.
func devResults(t *testing.T, e *engine.Engine, cat *engine.Catalog, qs []devQuery) []string {
	t.Helper()
	out := make([]string, len(qs))
	for i, q := range qs {
		rows, err := e.Query(t.Context(), cat, q.name, []expr.Value{expr.String(q.param)}, engine.QueryOptions{})
		out[i] = fmt.Sprintf("%s(%s): %v %v", q.name, q.param, rows, err)
	}
	return out
}

// Restarted after a change, dev creates the new tables, projects the events appended while
// it was down, and makes the new versions live, the additive ones split by time, while an
// application appends: the views then equal those of a store where every version was live
// from the start.
func TestDevRestart(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	r := s.start()
	s.appendViews(firstViews())
	s.waitLive(r)
	r.stop(t)
	more := []devPageView{{"u3", "/cart", 70, "2026-01-02T13:00:00Z"}}
	s.appendViews(more) // while dev is down

	next, err := os.ReadFile(filepath.Join(s.dir, "next", "shop.sql"))
	if err == nil {
		err = os.WriteFile(filepath.Join(s.dir, "dynaflux", "shop.sql"), next, 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
	s.generate()
	r = s.start()
	// Events on both sides of the time split.
	var clicks [][]expr.Value
	for i := range 12 {
		v := devPageView{fmt.Sprintf("u%d", i%3+1), []string{"/home", "/cart"}[i%2], uint64(10 * i), fmt.Sprintf("2026-01-03T%02d:00:00Z", i)}
		s.appendViews([]devPageView{v})
		more = append(more, v)
		if i%2 == 0 {
			click := []expr.Value{expr.String([]string{"buy", "sell"}[i%4/2]), devTime(t, v.at)}
			devAppend(t, s.engine, s.cat, "clicks", click)
			clicks = append(clicks, click)
		}
		time.Sleep(300 * time.Millisecond)
	}
	s.waitLive(r)

	oracle, err := access.Get().Bind(fluxtest.NewStore(t, fluxgen.LoadCatalog(string(s.data))), s.cat)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range slices.Concat(firstViews(), more) {
		devAppend(t, oracle, s.cat, "page_views", v.values(t))
	}
	devAppend(t, oracle, s.cat, "clicks", clicks...)
	var qs []devQuery
	for _, page := range []string{"/home", "/cart"} {
		qs = append(qs, devQuery{"PageStats", page}, devQuery{"PageUsers", page})
	}
	for _, user := range []string{"u1", "u2", "u3"} {
		qs = append(qs, devQuery{"UserViews", user})
	}
	qs = append(qs, devQuery{"Clicks", "buy"}, devQuery{"Clicks", "sell"})
	want := devResults(t, oracle, s.cat, qs)
	var got []string
	defer func() {
		if t.Failed() {
			t.Logf("results:\ngot  %q\nwant %q", got, want)
		}
	}()
	r.waitFor(t, "the oracle's results", func() bool {
		got = devResults(t, s.engine, s.cat, qs)
		return reflect.DeepEqual(got, want)
	})
	stats := result{0, "day                   views  total_ms\n2026-01-01T00:00:00Z  2      300\n" +
		"2026-01-02T00:00:00Z  1      300\n2026-01-03T00:00:00Z  6      300\n", s.head()}
	if got := s.run("query", "PageStats", "-param", "page=/home"); got != stats {
		t.Errorf("query PageStats:\ngot  %+v\nwant %+v", got, stats)
	}
	wantStates := map[string]string{
		"page_daily@v1": "live time", "page_users@v1": "live time",
		"page_daily@v2": "live time", "page_users@v2": "live time",
		"user_pages@v1": "live time", "button_clicks@v1": "live time",
	}
	if states := s.states(); !reflect.DeepEqual(states, wantStates) {
		t.Errorf("states %v, want %v", states, wantStates)
	}
	out := r.stop(t)
	p := s.prefix
	var created []string
	for _, line := range devLines(out.stderr) {
		if strings.HasPrefix(line, "created table ") {
			created = append(created, line)
		}
	}
	wantCreated := []string{
		"created table " + p + "devshop-stream-clicks",
		"created table " + p + "devshop-view-button_clicks-v1",
		"created table " + p + "devshop-view-page_daily-v2",
		"created table " + p + "devshop-view-page_users-v2",
		"created table " + p + "devshop-view-user_pages-v1",
	}
	if out.status != 0 || !slices.Equal(created, wantCreated) {
		t.Errorf("dev: status %d, created\n%s\nwant status 0, created\n%s", out.status, strings.Join(created, "\n"), strings.Join(wantCreated, "\n"))
	}
}

// -reset deletes the store's tables, with their events, and dev starts on an empty store.
func TestDevReset(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	r := s.start()
	s.appendViews(firstViews())
	s.waitLive(r)
	r.waitFor(t, "every event projected", func() bool {
		return s.run("query", "PageUsers", "-param", "page=/cart").status == 0
	})
	r.stop(t)

	r = s.start("-reset")
	s.waitLive(r)
	qs := []devQuery{{"PageStats", "/home"}, {"PageUsers", "/cart"}}
	want := []string{"PageStats(/home): [] <nil>", "PageUsers(/cart): [] dynaflux: no row found"}
	if got := devResults(t, s.engine, s.cat, qs); !slices.Equal(got, want) {
		t.Errorf("after -reset:\ngot  %q\nwant %q", got, want)
	}
	out := r.stop(t)
	p := s.prefix
	tables := []string{p + "devshop-stream-page_views", p + "devshop-meta", p + "devshop-view-page_daily-v1", p + "devshop-view-page_users-v1"}
	var wantHead []string
	for _, verb := range []string{"deleted table ", "created table "} {
		for _, name := range tables {
			wantHead = append(wantHead, verb+name)
		}
	}
	if got := devLines(out.stderr); out.status != 0 || len(got) < 9 || !slices.Equal(got[1:9], wantHead) {
		t.Errorf("dev -reset: status %d, stderr\n%s\nwant status 0, and after the header\n%s", out.status, out.stderr, strings.Join(wantHead, "\n"))
	}
}

// backlogSQL is dev.txtar's SQL with an additive view and an idempotent one added, which
// sort before and after its views.
const backlogSQL = `
CREATE STREAM page_views (
    user_id     String,
    page        String,
    duration_ms UInt32
)
PRIMARY KEY (user_id);

CREATE MATERIALIZED VIEW a_counts
ORDER BY (page)
AS SELECT page, count() AS views
FROM page_views
GROUP BY page;

CREATE MATERIALIZED VIEW page_daily
ORDER BY (page, day)
PRIMARY KEY (page)
AS SELECT page, toStartOfDay(event_time) AS day, count() AS views
FROM page_views
GROUP BY page, day;

CREATE MATERIALIZED VIEW page_users
ORDER BY (page)
PRIMARY KEY (page)
AS SELECT page, uniqExact(user_id) AS visitors, max(duration_ms) AS longest
FROM page_views
GROUP BY page;

CREATE MATERIALIZED VIEW z_peaks
ORDER BY (user_id)
AS SELECT user_id, max(duration_ms) AS longest
FROM page_views
GROUP BY user_id;

-- name: PageStats :many
SELECT day, views FROM page_daily WHERE page = {page:String} ORDER BY day;

-- name: PageUsers :one
SELECT visitors, longest FROM page_users WHERE page = {page:String};
`

// An additive version activated just before an idempotent one, both over a backlog of
// events, while an application appends: the idempotent one's steps, in chunks of 200 rows,
// leave the projector time to keep within the additive one's late bound, so both go live.
func TestDevFillsABacklog(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	r := s.start()
	s.waitLive(r)
	r.stop(t)
	const backlog, each = 1000, 25
	for i := 0; i < backlog; i += each {
		rows := make([][]expr.Value, each)
		for k := range rows {
			rows[k] = devPageView{fmt.Sprintf("user-%05d", i+k), "/home", uint64(i + k), "2026-01-01T10:00:00Z"}.values(t)
		}
		devAppend(t, s.engine, s.cat, "page_views", rows...)
	}
	if err := os.WriteFile(filepath.Join(s.dir, "dynaflux", "shop.sql"), []byte(backlogSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	s.generate()
	r = s.start()
	stop, appended := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(appended)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			devAppend(t, s.engine, s.cat, "page_views", devPageView{fmt.Sprintf("live-%d", i), "/cart", 5, "2026-01-02T10:00:00Z"}.values(t))
		}
	}()
	s.waitLive(r)
	close(stop)
	<-appended
	r.stop(t)
	want := map[string]string{"a_counts@v1": "live time", "page_daily@v1": "live time", "page_users@v1": "live time", "z_peaks@v1": "live none"}
	if states := s.states(); !reflect.DeepEqual(states, want) {
		t.Errorf("states %v, want %v", states, want)
	}
}

// proxy forwards a loopback port to DynamoDB Local until it is closed.
type proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newProxy(t *testing.T, endpoint string) *proxy {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	t.Cleanup(p.close)
	go func() {
		for {
			down, aerr := ln.Accept()
			if aerr != nil {
				return // closed
			}
			up, derr := net.Dial("tcp", u.Host)
			if derr != nil {
				_ = down.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, down, up)
			p.mu.Unlock()
			// Each copy ends with an error when either side closes, as close does.
			go func() { _, _ = io.Copy(up, down); _ = up.Close() }()
			go func() { _, _ = io.Copy(down, up); _ = down.Close() }()
		}
	}()
	return p
}

func (p *proxy) endpoint() string { return "http://" + p.ln.Addr().String() }

// close stops accepting and drops every connection.
func (p *proxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

// DynamoDB Local going away while dev runs stops dev with exit status 2 and the command that
// starts it, as at startup.
func TestDevLosesDynamoDBLocal(t *testing.T) {
	t.Parallel()
	s := newDevStore(t)
	p := newProxy(t, s.endpoint)
	s.endpoint = p.endpoint()
	r := s.start()
	r.waitFor(t, "the catch-up", func() bool { return strings.Contains(r.stderr.String(), "caught up\n") })
	p.close()
	select {
	case <-r.done:
	case <-time.After(devWait):
		t.Fatalf("dev didn't stop without DynamoDB Local; it printed\n%s", r.stderr.String())
	}
	head := "dynaflux dev: DynamoDB Local stopped answering at " + s.endpoint + ": "
	_, port, _ := strings.Cut(strings.TrimPrefix(s.endpoint, "http://"), ":")
	tail := "\n  hint: start it with docker run --rm -p 127.0.0.1:" + port + ":8000 -e DDB_LOCAL_TELEMETRY=0 amazon/dynamodb-local:3.3.2 -jar DynamoDBLocal.jar -sharedDb -inMemory -disableTelemetry, then run dynaflux dev again\n"
	stderr := r.stderr.String()
	if r.status != 2 || !strings.Contains(stderr, "\n"+head) || !strings.HasSuffix(stderr, tail) {
		t.Errorf("dev: status %d, stderr\n%s\nwant status 2 and\n%s…%s", r.status, stderr, head, tail)
	}
}
