// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/store"
)

// operatorCommands are the commands that reach AWS: "Every command" in infra.md §9.8.
var operatorCommands = []string{"status", "diff", "inspect", "query", "backfill", "retire", "import"} //nolint:gochecknoglobals // test fixture

// call is one AWS request of an operator command: its IAM action, the ARN that IAM
// authorizes it on, and the s3:prefix of a listing.
type call struct {
	command, action, arn, prefix string
}

func (c call) String() string {
	s := c.command + " to send " + c.action + " on " + c.arn
	if c.prefix != "" {
		s += " with s3:prefix " + c.prefix
	}
	return s
}

// recorder notes the calls of one run. Its clients forward every method of the CLI's AWS
// seam without embedding it, so a new method doesn't compile until it is noted here.
type recorder struct {
	mu      sync.Mutex
	command string
	calls   []call
}

func (r *recorder) note(action, arn, prefix string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call{r.command, action, arn, prefix})
}

func tableARN(region, name string) string {
	return "arn:aws:dynamodb:" + region + ":" + account + ":table/" + name
}

// wrap returns env with each of its clients noting its calls in r. The stream tables'
// replicas and shards are read by the awsbackfill adapters themselves, from env's fakes over
// HTTP, so the calls noted are the ones they sign.
func (r *recorder) wrap(env cli.Env) cli.Env {
	out := env
	out.Account = account
	out.Backends, out.StepFunctions, out.AWS = map[string]store.Backend{}, map[string]cli.StepFunctions{}, map[string]cli.AWS{}
	out.Tables, out.Units = map[string]backfill.Tables{}, map[string]backfill.Lister{}
	fake := adapterFake{r: r, regions: slices.Sorted(maps.Keys(env.Backends)), born: map[string]*fakeReplicas{}, units: env.Units}
	for region, t := range env.Tables {
		fake.born[region] = t.(*fakeReplicas)
	}
	for region, b := range env.Backends {
		out.Backends[region] = recBackend{b, r, region}
		if c, ok := env.StepFunctions[region]; ok {
			out.StepFunctions[region] = recSFN{c, r}
		}
		if c, ok := env.AWS[region]; ok {
			out.AWS[region] = recAWS{c, r, region}
		}
		cfg := adapterConfig(fake, region)
		if _, ok := env.Tables[region]; ok {
			out.Tables[region] = awsbackfill.NewReplicas(cfg)
		}
		if _, ok := env.Units[region]; ok {
			out.Units[region] = awsbackfill.NewUnits(dynamodb.NewFromConfig(cfg), dynamodbstreams.NewFromConfig(cfg))
		}
	}
	return out
}

// recBackend notes the DynamoDB action of each store call, as the dynamo backend sends it.
type recBackend struct {
	next   store.Backend
	r      *recorder
	region string
}

func (b recBackend) Get(ctx context.Context, table string, k store.Key, consistent bool) (store.Item, error) {
	b.r.note("dynamodb:GetItem", tableARN(b.region, table), "")
	return b.next.Get(ctx, table, k, consistent)
}

func (b recBackend) BatchGet(ctx context.Context, table string, ks []store.Key, consistent bool) ([]store.Item, error) {
	b.r.note("dynamodb:BatchGetItem", tableARN(b.region, table), "")
	return b.next.BatchGet(ctx, table, ks, consistent)
}

func (b recBackend) Query(ctx context.Context, table string, q store.Query) iter.Seq2[store.Item, error] {
	b.r.note("dynamodb:Query", tableARN(b.region, table), "")
	return b.next.Query(ctx, table, q)
}

func (b recBackend) ScanPage(ctx context.Context, table string, sc store.Scan, start *store.Key) ([]store.Item, *store.Key, error) {
	b.r.note("dynamodb:Scan", tableARN(b.region, table), "")
	return b.next.ScanPage(ctx, table, sc, start)
}

func (b recBackend) Put(ctx context.Context, table string, it store.Item, c store.Cond) error {
	b.r.note("dynamodb:PutItem", tableARN(b.region, table), "")
	return b.next.Put(ctx, table, it, c)
}

// Transact notes PutItem on each put's table: IAM has no transaction action.
func (b recBackend) Transact(ctx context.Context, puts []store.TxPut) error {
	for _, p := range puts {
		b.r.note("dynamodb:PutItem", tableARN(b.region, p.Table), "")
	}
	return b.next.Transact(ctx, puts)
}

type recSFN struct {
	next cli.StepFunctions
	r    *recorder
}

func (c recSFN) StartExecution(ctx context.Context, in *sfn.StartExecutionInput, opts ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	c.r.note("states:StartExecution", aws.ToString(in.StateMachineArn), "")
	return c.next.StartExecution(ctx, in, opts...)
}

func (c recSFN) DescribeExecution(ctx context.Context, in *sfn.DescribeExecutionInput, opts ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	c.r.note("states:DescribeExecution", aws.ToString(in.ExecutionArn), "")
	return c.next.DescribeExecution(ctx, in, opts...)
}

func (c recSFN) StopExecution(ctx context.Context, in *sfn.StopExecutionInput, opts ...func(*sfn.Options)) (*sfn.StopExecutionOutput, error) {
	c.r.note("states:StopExecution", aws.ToString(in.ExecutionArn), "")
	return c.next.StopExecution(ctx, in, opts...)
}

type recAWS struct {
	next   cli.AWS
	r      *recorder
	region string
}

func (c recAWS) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	c.r.note("dynamodb:DescribeTable", tableARN(c.region, aws.ToString(in.TableName)), "")
	return c.next.DescribeTable(ctx, in, opts...)
}

func (c recAWS) ListEventSourceMappings(ctx context.Context, in *lambda.ListEventSourceMappingsInput, opts ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error) {
	c.r.note("lambda:ListEventSourceMappings", "*", "")
	return c.next.ListEventSourceMappings(ctx, in, opts...)
}

func (c recAWS) GetMetricData(ctx context.Context, in *cloudwatch.GetMetricDataInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	c.r.note("cloudwatch:GetMetricData", "*", "")
	return c.next.GetMetricData(ctx, in, opts...)
}

func (c recAWS) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	c.r.note("s3:ListBucket", "arn:aws:s3:::"+aws.ToString(in.Bucket), aws.ToString(in.Prefix))
	return c.next.ListObjectsV2(ctx, in, opts...)
}

func (c recAWS) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.r.note("s3:GetObject", "arn:aws:s3:::"+aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key), "")
	return c.next.GetObject(ctx, in, opts...)
}

// StopQueryExecution notes the backfill workgroup, where IAM authorizes the call: a job's
// export runs there.
func (c recAWS) StopQueryExecution(ctx context.Context, in *athena.StopQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error) {
	c.r.note("athena:StopQueryExecution", "arn:aws:athena:"+c.region+":"+account+":workgroup/"+shop.BackfillWorkGroup(), "")
	return c.next.StopQueryExecution(ctx, in, opts...)
}

// adapterFake answers the requests that the awsbackfill adapters sign, DescribeTable and
// DescribeStream, in every region: each stream table is a Global Table with a replica in each
// of regions, born as born says, and its stream has the shards that units lists. It notes
// each request in r, in the region it was signed for.
type adapterFake struct {
	r       *recorder
	regions []string
	born    map[string]*fakeReplicas // by region; a table without a time is new
	units   map[string]backfill.Lister
}

var signedRegion = regexp.MustCompile(`Credential=[^/]+/\d+/([^/]+)/`)

func (f adapterFake) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var in struct {
		TableName string `json:"TableName"`
		StreamArn string `json:"StreamArn"`
	}
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	region := signedRegion.FindStringSubmatch(req.Header.Get("Authorization"))[1]
	_, op, _ := strings.Cut(req.Header.Get("X-Amz-Target"), ".")
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	switch op {
	case "DescribeTable":
		f.r.note("dynamodb:DescribeTable", tableARN(region, in.TableName), "")
		born := time.Now()
		if b, ok := f.born[region]; ok && !b.born[in.TableName].IsZero() {
			born = b.born[in.TableName]
		}
		var replicas []string
		for _, r := range f.regions {
			replicas = append(replicas, `{"RegionName":"`+r+`"}`)
		}
		fmt.Fprintf(w, `{"Table":{"TableName":%q,"CreationDateTime":%d,"LatestStreamArn":%q,"StreamSpecification":{"StreamEnabled":true,`+
			`"StreamViewType":"NEW_AND_OLD_IMAGES"},"Replicas":[%s]}}`, in.TableName, born.Unix(),
			tableARN(region, in.TableName)+"/stream/2000-01-01T00:00:00.000", strings.Join(replicas, ","))
	case "DescribeStream":
		f.r.note("dynamodb:DescribeStream", in.StreamArn, "")
		table, _, _ := strings.Cut(strings.TrimPrefix(in.StreamArn, tableARN(region, "")), "/")
		var shards []string
		if l, ok := f.units[region]; ok {
			ids, _ := l.Units(req.Context(), table)
			for _, id := range ids {
				shards = append(shards, `{"ShardId":"`+id+`"}`)
			}
		}
		fmt.Fprintf(w, `{"StreamDescription":{"StreamArn":%q,"Shards":[%s]}}`, in.StreamArn, strings.Join(shards, ","))
	default:
		http.Error(w, "unexpected "+op, http.StatusBadRequest)
	}
}

// inProcess is an HTTP client that serves each request with h, opening no socket.
type inProcess struct{ h http.Handler }

func (c inProcess) Do(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

// adapterConfig configures the SDK clients of the awsbackfill adapters in region, with fake
// credentials and h answering in process.
func adapterConfig(h http.Handler, region string) aws.Config {
	return aws.Config{
		Region: region, BaseEndpoint: aws.String("http://127.0.0.1:1"), HTTPClient: inProcess{h}, RetryMaxAttempts: 1,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDFAKE", SecretAccessKey: "fake", Source: "test"}, nil
		}),
	}
}

// operatorRun is a run of an operator command on a deployed store that setup makes, in a
// synctest bubble.
type operatorRun struct {
	name   string
	args   []string
	status int
	setup  func(t *testing.T) (dir string, env cli.Env)
}

func operatorRuns() []operatorRun {
	east := []string{"-region", "us-east-1"}
	return []operatorRun{
		{"status", []string{"status", "-verbose"}, 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newStatusStore(t)
			s.job(t, "us-east-1", types.ExecutionStatusRunning)
			s.gap("uuid/shardId-0001/2000/01/01/2000-01-01T00.30.00-a", time.Now().Add(-time.Hour))
			return s.dir, s.env()
		}},
		{"diff", []string{"diff"}, 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newDiffStore(t)
			return s.dir, cli.Env{Region: "us-east-1", Backends: s.backends}
		}},
		{"inspect a stream", slices.Concat([]string{"inspect", "page_views", "-key", "user_id=u1"}, east), 0, liveDeployed},
		{"inspect a missing row", slices.Concat([]string{"inspect", "user_pages", "-key", "user_id=u1"}, east), 1, liveDeployed},
		{"inspect a split partition", slices.Concat([]string{"inspect", "page_daily", "-raw", "-key", "page=/home"}, east), 0, liveDeployed},
		{"query a split view", slices.Concat([]string{"query", "PageStats", "-param", "page=/home", "-param", "from=1999-12-30", "-param", "to=1999-12-31"}, east), 0, liveDeployed},
		{"query rows by key", slices.Concat([]string{"query", "UserViews", "-param", "users=['u1','u2']"}, east), 0, liveDeployed},
		{"query a stream", slices.Concat([]string{"query", "UserEvents", "-param", "users=['u1']"}, east), 0, liveDeployed},
		{"backfill new jobs", []string{"backfill", "-all", "-yes"}, 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newBackfillStore(t)
			// Every replica of signups is new, so us-east-1 checks that it is empty and records
			// its origin. page_views's archive has an origin of two days, whose hours the
			// activity check reads, and a gap since.
			s.tables["us-west-2"].born[catalog.Naming{Store: "shop"}.StreamTable("signups")] = time.Now()
			s.origin(t, "us-east-1", "page_views", time.Now().Add(-48*time.Hour))
			s.gap("us-east-1", "page_views", time.Now().Add(-time.Hour))
			return s.env()
		}},
		{"backfill resume", slices.Concat([]string{"backfill", "page_seen@v1"}, east), 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newBackfillStore(t)
			s.job(t, types.ExecutionStatusFailed)
			return s.env()
		}},
		{"backfill cancel", slices.Concat([]string{"backfill", "-cancel", "page_seen@v1"}, east), 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newBackfillStore(t)
			s.job(t, types.ExecutionStatusRunning)
			return s.env()
		}},
		{"backfill cancel an export", slices.Concat([]string{"backfill", "-cancel", "plan_counts@v1"}, east), 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newBackfillStore(t)
			s.exportJob(t)
			return s.env()
		}},
		{"retire", []string{"retire", "page_counts@v1", "-yes"}, 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newRetireStore(t)
			return s.dir, cli.Env{Region: "us-east-1", Backends: s.backends}
		}},
		{"import", []string{"import", "-region", "us-east-1", "page_views", "-file", "input/views.jsonl", "-yes"}, 0, func(t *testing.T) (string, cli.Env) {
			t.Helper()
			s := newImportStore(t)
			return s.dir, cli.Env{Backends: map[string]store.Backend{"us-east-1": s.b}}
		}},
		{"import into a young stream", []string{"import", "-region", "us-east-1", "page_views", "-file", "input/views.jsonl", "-yes"}, 1,
			func(t *testing.T) (string, cli.Env) {
				t.Helper()
				s := newYoungImportStore(t)
				return s.dir, s.env()
			}},
	}
}

// liveDeployed is the deployed store of TestDeployed with user_pages@v1 live too.
func liveDeployed(t *testing.T) (string, cli.Env) {
	t.Helper()
	d := newDeployed(t)
	makeLive(t, d.meta, "user_pages@v1")
	return d.dir, cli.Env{Region: "us-west-2", Backends: map[string]store.Backend{"us-east-1": d.b}}
}

func (s *backfillStore) env() (string, cli.Env) { return s.dir, s.cliEnv(false) }

// operatorCalls runs every operator run, and returns their distinct calls in order.
func operatorCalls(t *testing.T) []call {
	t.Helper()
	seen := map[call]bool{}
	for _, o := range operatorRuns() {
		synctest.Test(t, func(t *testing.T) {
			dir, env := o.setup(t)
			rec := &recorder{command: o.args[0]}
			var stdout, stderr strings.Builder
			if status := cli.RunEnv(t.Context(), o.args, nil, &stdout, &stderr, dir, rec.wrap(env)); status != o.status {
				t.Fatalf("%s: status %d, want %d; stderr:\n%s", o.name, status, o.status, stderr.String())
			}
			for _, c := range rec.calls {
				seen[c] = true
			}
		})
	}
	return slices.SortedFunc(maps.Keys(seen), func(a, b call) int {
		return cmp.Or(cmp.Compare(a.command, b.command), cmp.Compare(a.action, b.action), cmp.Compare(a.arn, b.arn), cmp.Compare(a.prefix, b.prefix))
	})
}

// foreignCalls is what calls name for another store and for the store under another prefix,
// and in the bucket outside aws/lambda/, where the archive is.
func foreignCalls(calls []call) []call {
	var out []call
	for _, c := range calls {
		if c.arn == "*" {
			continue
		}
		for _, base := range []string{"dev-shop", "billing"} {
			f := c
			f.arn = strings.Replace(c.arn, "shop", base, 1)
			out = append(out, f)
		}
		if c.action == "s3:GetObject" || c.action == "s3:ListBucket" {
			f := c
			f.arn = strings.Replace(c.arn, "/aws/lambda/", "/warehouse/", 1)
			if c.prefix != "" {
				f.prefix = "warehouse/"
			}
			out = append(out, f)
		}
	}
	return out
}

// operatorRow is a row of infra.md §9.8: actions that commands send on resources.
type operatorRow struct {
	line      int
	actions   []string
	resources []string // ARN patterns, as infra.md writes them
	condition []string // a condition key and its value, or nil
	commands  []string
}

var (
	codeSpan    = regexp.MustCompile("`([^`]+)`")
	placeholder = regexp.MustCompile(`\{[A-Za-z]+\}|\*`)
)

// operatorTable reads the table of infra.md §9.8.
func operatorTable(t *testing.T) []operatorRow {
	t.Helper()
	data, err := os.ReadFile("../../docs/spec/infra.md")
	if err != nil {
		t.Fatal(err)
	}
	spans := func(cell string) []string {
		var out []string
		for _, m := range codeSpan.FindAllStringSubmatch(cell, -1) {
			out = append(out, m[1])
		}
		return out
	}
	lines := strings.Split(string(data), "\n")
	start := slices.IndexFunc(lines, func(l string) bool { return strings.HasPrefix(l, "### 9.8 ") })
	var rows []operatorRow
	for i := start + 1; start >= 0 && i < len(lines) && !strings.HasPrefix(lines[i], "#"); i++ {
		cells := strings.Split(lines[i], "|")
		if len(cells) != 6 || !strings.HasPrefix(strings.TrimSpace(cells[1]), "`") {
			continue
		}
		row := operatorRow{line: i + 1, resources: spans(cells[2]), condition: spans(cells[3]), commands: spans(cells[4])}
		if strings.TrimSpace(cells[4]) == "Every command" {
			row.commands = operatorCommands
		}
		service := ""
		for _, a := range spans(cells[1]) {
			if s, _, ok := strings.Cut(a, ":"); ok {
				service = s
			} else {
				a = service + ":" + a
			}
			row.actions = append(row.actions, a)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		t.Fatal("infra.md has no table in section 9.8")
	}
	return rows
}

// arnRegexp matches what pattern names for the test's store, shop, without a prefix.
func arnRegexp(pattern string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	last := 0
	for _, m := range placeholder.FindAllStringIndex(pattern, -1) {
		b.WriteString(regexp.QuoteMeta(pattern[last:m[0]]))
		switch p := pattern[m[0]:m[1]]; p {
		case "{partition}":
			b.WriteString("aws")
		case "{region}":
			b.WriteString(`[a-z]{2}(-[a-z]+)+-[0-9]`)
		case "{account}":
			b.WriteString(account)
		case "{base}":
			b.WriteString("shop")
		case "{stream}", "{view}":
			b.WriteString(`[a-z][a-z0-9_]*`)
		case "{N}":
			b.WriteString(`[1-9][0-9]*`)
		case "*":
			b.WriteString(".*")
		default:
			return nil, fmt.Errorf("unknown placeholder %s in %s", p, pattern)
		}
		last = m[1]
	}
	return regexp.Compile(b.String() + regexp.QuoteMeta(pattern[last:]) + "$")
}

// allows returns the resource of r that allows c, or "".
func (r operatorRow) allows(c call) (string, error) {
	if !slices.Contains(r.commands, c.command) || !slices.Contains(r.actions, c.action) {
		return "", nil
	}
	switch {
	case len(r.condition) == 2 && r.condition[0] == "s3:prefix":
		re, err := arnRegexp(r.condition[1])
		if err != nil || !re.MatchString(c.prefix) {
			return "", err
		}
	case len(r.condition) != 0:
		return "", fmt.Errorf("infra.md:%d: unknown condition %q", r.line, r.condition)
	}
	for _, p := range r.resources {
		re, err := arnRegexp(p)
		if err != nil {
			return "", fmt.Errorf("infra.md:%d: %w", r.line, err)
		}
		if re.MatchString(c.arn) {
			return p, nil
		}
	}
	return "", nil
}

// checkOperators compares calls with rows both ways: a call that no row allows for its
// command would be AccessDenied on AWS, while a row that allows a call of foreign, or lists
// an action for a command that never sends it or a resource that no call names, grants
// too much.
func checkOperators(rows []operatorRow, calls, foreign []call) error {
	var errs []error
	used := map[string]bool{}
	for _, c := range calls {
		uses, err := allowing(rows, c)
		if err != nil {
			return err
		}
		if len(uses) == 0 {
			errs = append(errs, fmt.Errorf("no row allows %s", c))
		}
		for _, u := range uses {
			used[u] = true
		}
	}
	for _, c := range foreign {
		for _, r := range rows {
			p, err := r.allows(c)
			if err != nil {
				return err
			}
			if p != "" {
				errs = append(errs, fmt.Errorf("infra.md:%d allows %s, another store's or environment's", r.line, c))
			}
		}
	}
	return errors.Join(append(errs, unused(rows, used)...)...)
}

// allowing returns what c uses of the rows that allow it: each row's index with c's action
// and command, and with the resource that names c's ARN.
func allowing(rows []operatorRow, c call) ([]string, error) {
	var uses []string
	for i, r := range rows {
		p, err := r.allows(c)
		if err != nil {
			return nil, err
		}
		if p != "" {
			uses = append(uses, fmt.Sprint(i, c.action, c.command), fmt.Sprint(i, p))
		}
	}
	return uses, nil
}

// unused reports what rows list that no call uses.
func unused(rows []operatorRow, used map[string]bool) []error {
	var errs []error
	for i, r := range rows {
		for _, action := range r.actions {
			for _, command := range r.commands {
				if !used[fmt.Sprint(i, action, command)] {
					errs = append(errs, fmt.Errorf("infra.md:%d lists %s for %s, which never sends it there", r.line, action, command))
				}
			}
		}
		for _, p := range r.resources {
			if !used[fmt.Sprint(i, p)] {
				errs = append(errs, fmt.Errorf("infra.md:%d lists %s, which no call names", r.line, p))
			}
		}
	}
	return errs
}

// infra.md §9.8 lists what each operator command sends to AWS, on the resources it names,
// and nothing else.
func TestOperatorPermissions(t *testing.T) {
	t.Parallel()
	calls := operatorCalls(t)
	if err := checkOperators(operatorTable(t), calls, foreignCalls(calls)); err != nil {
		t.Error(err)
	}
}

// Each control breaks the table one way, and the check must name it.
func TestOperatorPermissionsControls(t *testing.T) {
	t.Parallel()
	calls := operatorCalls(t)
	foreign := foreignCalls(calls)
	const view = "arn:{partition}:dynamodb:{region}:{account}:table/{base}-view-{view}-v{N}"
	tests := []struct {
		name, action, resource string // the row changed: the first with action, naming resource
		change                 func(r *operatorRow)
		want                   string
	}{
		{"an action a command sends", "states:StopExecution", "execution", func(r *operatorRow) { r.actions = nil },
			"no row allows backfill to send states:StopExecution on arn:aws:states:us-east-1:123456789012:execution:shop-backfill:page-seen-v1-0a1b2c-a1"},
		{"a command that sends it", "dynamodb:PutItem", "stream", func(r *operatorRow) { r.commands = nil },
			"no row allows import to send dynamodb:PutItem on arn:aws:dynamodb:us-east-1:123456789012:table/shop-stream-page_views"},
		{"an action no command sends", "dynamodb:PutItem", "meta", func(r *operatorRow) {
			r.actions = append(slices.Clone(r.actions), "dynamodb:DeleteItem")
		}, "lists dynamodb:DeleteItem for retire, which never sends it there"},
		{"a command that doesn't send it", "dynamodb:PutItem", "meta", func(r *operatorRow) {
			r.commands = append(slices.Clone(r.commands), "query")
		}, "lists dynamodb:PutItem for query, which never sends it there"},
		{"a resource no call names", "dynamodb:Scan", "stream", func(r *operatorRow) {
			r.resources = append(slices.Clone(r.resources), view)
		}, "lists " + view + ", which no call names"},
		{"one region", "dynamodb:DescribeTable", "stream", func(r *operatorRow) {
			r.resources = []string{"arn:{partition}:dynamodb:us-east-1:{account}:table/{base}-stream-{stream}"}
		}, "no row allows backfill to send dynamodb:DescribeTable on arn:aws:dynamodb:us-west-2:123456789012:table/shop-stream-page_views"},
		{"every table", "dynamodb:GetItem", "meta", func(r *operatorRow) {
			r.resources = []string{"arn:{partition}:dynamodb:{region}:{account}:table/*-meta"}
		}, "allows retire to send dynamodb:GetItem on arn:aws:dynamodb:us-west-2:123456789012:table/dev-shop-meta, another"},
		{"the whole bucket", "s3:GetObject", "aws/lambda", func(r *operatorRow) {
			r.resources = []string{"arn:{partition}:s3:::{base}-{account}-{region}-an/*"}
		}, "allows status to send s3:GetObject on arn:aws:s3:::shop-123456789012-us-east-1-an/warehouse/uuid/"},
		{"any listing", "s3:ListBucket", "-an", func(r *operatorRow) { r.condition = nil },
			"allows status to send s3:ListBucket on arn:aws:s3:::shop-123456789012-us-east-1-an with s3:prefix warehouse/, another"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rows := slices.Clone(operatorTable(t))
			i := slices.IndexFunc(rows, func(r operatorRow) bool {
				return slices.Contains(r.actions, tt.action) && strings.Contains(strings.Join(r.resources, " "), tt.resource)
			})
			if i < 0 {
				t.Fatalf("infra.md §9.8 has no row of %s on %s", tt.action, tt.resource)
			}
			tt.change(&rows[i])
			if err := checkOperators(rows, calls, foreign); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the check fails with %v, want %q", err, tt.want)
			}
		})
	}
}
