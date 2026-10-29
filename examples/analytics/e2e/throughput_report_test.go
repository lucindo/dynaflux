// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// No build tag: make test checks the throughput report on a recorded run, since the report
// is arithmetic over the run's result file, CloudWatch's numbers and explain's model, and
// needs no store.

package e2e_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/explain"
	"github.com/lucindo/dynaflux/internal/validate"
)

// The e2e store is the example with e2eSQL added, as scripts/e2e/deploy.sh generates it.
const (
	exampleDir = ".."
	e2eSQL     = "testdata/e2e.sql"
)

// compile compiles the example with an SQL file of testdata added, in regions when it isn't
// empty, as deploy.sh does with E2E_REGIONS.
func compile(t *testing.T, sql string, regions []string) (*config.Config, *catalog.Catalog) {
	t.Helper()
	fsys := fstest.MapFS{}
	example := os.DirFS(exampleDir)
	for _, root := range []string{"dynaflux.yaml", catalog.LockFileName, "dynaflux"} {
		err := fs.WalkDir(example, root, func(name string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := fs.ReadFile(example, name)
			fsys[name] = &fstest.MapFile{Data: data}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(regions) > 0 {
		line := regexp.MustCompile(`(?m)^regions: .*$`)
		yaml := fsys["dynaflux.yaml"].Data
		if !line.Match(yaml) {
			t.Fatal("the example's dynaflux.yaml has no regions line to replace")
		}
		fsys["dynaflux.yaml"].Data = line.ReplaceAll(yaml, []byte("regions: ["+strings.Join(regions, ", ")+"]"))
	}
	data, err := os.ReadFile(sql)
	if err != nil {
		t.Fatal(err)
	}
	fsys["dynaflux/"+path.Base(sql)] = &fstest.MapFile{Data: data}
	cfg, def, ds := validate.Load(fsys, exampleDir)
	if def == nil {
		var b bytes.Buffer
		if err = diag.Write(&b, ds); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("the example with %s doesn't compile:\n%s", sql, b.Bytes())
	}
	return cfg, def
}

// throughputResult is what TestThroughput writes to -throughput.out as it goes. The report
// reads it and CloudWatch, nothing else.
type throughputResult struct {
	Target string `json:"target"`
	// Phase is how far the run got: loading, checking or done.
	Phase   string   `json:"phase"`
	Regions []string `json:"regions"`
	// Base is the store's prefixed name, the dimension of the runtime's metrics.
	Base     string       `json:"base"`
	Workload loadWorkload `json:"workload"`
	// Start is when the load began. The window, after the warm-up, is what the report
	// measures; on AWS it starts on a whole minute and lasts whole minutes, as CloudWatch's
	// datapoints do.
	Start       time.Time      `json:"start"`
	WindowStart time.Time      `json:"window_start"`
	WindowEnd   time.Time      `json:"window_end"`
	PerRegion   []regionResult `json:"per_region"`
	Probes      []probeResult  `json:"probes"`
	// TagsPerEvent and UniqNew are explain's -pass tag_hourly and -uniq-new page_daily, as
	// the window's appends measured them.
	TagsPerEvent float64 `json:"tags_per_event"`
	UniqNew      float64 `json:"uniq_new"`
	// Failures are why the run failed: empty when it passed.
	Failures []string `json:"failures"`
}

type loadWorkload struct {
	Rate    float64 `json:"rate"` // events per second, all regions together
	Workers int     `json:"workers"`
	Users   int     `json:"users"`
	Pages   int     `json:"pages"`
	Tags    int     `json:"tags"`
}

// regionResult is one region's resources, which the report reads the metrics of, and what
// its load generator saw.
type regionResult struct {
	Name        string            `json:"name"`
	StreamTable string            `json:"stream_table"`
	MetaTable   string            `json:"meta_table"`
	ViewTables  map[string]string `json:"view_tables"` // by view
	Function    string            `json:"function"`
	Mapping     string            `json:"mapping"` // the event source mapping's UUID, on AWS
	Archive     string            `json:"archive"` // the Firehose stream
	Shards      int               `json:"shards"`  // open shards of the stream table, after the load
	// Appended and Missed count the window's appends: those that succeeded, and those that
	// no worker was free to send. Unknown counts failed appends of the whole run, which may
	// or may not be stored.
	Appended int64             `json:"appended"`
	Missed   int64             `json:"missed"`
	Unknown  int64             `json:"unknown"`
	Errors   map[string]int64  `json:"errors"`        // failed appends by class
	Samples  map[string]string `json:"error_samples"` // the first message of each class
	// Latency is the client's: from each append's scheduled time to its return, so that
	// time spent waiting for a worker counts.
	Latency percentiles `json:"latency_ms"`
}

type percentiles struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

// probeResult is how long an append in Origin took to show in Observer's views, or Missed.
type probeResult struct {
	Origin   string    `json:"origin"`
	Observer string    `json:"observer"`
	Sent     time.Time `json:"sent"`
	LagMS    float64   `json:"lag_ms"`
	Missed   bool      `json:"missed,omitzero"`
}

// newPercentiles summarizes samples, which it sorts, by nearest rank.
func newPercentiles(samples []float64) percentiles {
	if len(samples) == 0 {
		return percentiles{}
	}
	slices.Sort(samples)
	rank := func(p float64) float64 {
		return samples[max(0, int(math.Ceil(p*float64(len(samples))))-1)]
	}
	return percentiles{N: len(samples), P50: rank(0.5), P90: rank(0.9), P99: rank(0.99), Max: samples[len(samples)-1]}
}

// metricQuery is a CloudWatch statistic over the window. ID names it in the report and in
// GetMetricData, which allows letters, digits and underscores.
type metricQuery struct {
	ID, Namespace, Name, Stat string
	Dims                      []dimension
}

type dimension struct{ Name, Value string }

const (
	statSum = "Sum"
	statAvg = "Average"
	statMax = "Maximum"
)

// metricQueries returns what the report reads of a region, in which every view table is
// regional and the stream table one replica of a global table.
func metricQueries(res *throughputResult, rr *regionResult) []metricQuery {
	fn := []dimension{{"FunctionName", rr.Function}}
	archive := []dimension{{"DeliveryStreamName", rr.Archive}}
	store := []dimension{{catalog.MetricDimensionStore, res.Base}}
	qs := []metricQuery{
		{"invocations", "AWS/Lambda", "Invocations", statSum, fn},
		{"lambda_errors", "AWS/Lambda", "Errors", statSum, fn},
		{"lambda_throttles", "AWS/Lambda", "Throttles", statSum, fn},
		{"duration_avg", "AWS/Lambda", "Duration", statAvg, fn},
		{"duration_p50", "AWS/Lambda", "Duration", "p50", fn},
		{"duration_p99", "AWS/Lambda", "Duration", "p99", fn},
		{"duration_max", "AWS/Lambda", "Duration", statMax, fn},
		{"concurrency_max", "AWS/Lambda", "ConcurrentExecutions", statMax, fn},
		{"iterator_age_avg", "AWS/Lambda", "IteratorAge", statAvg, fn},
		{"iterator_age_max", "AWS/Lambda", "IteratorAge", statMax, fn},
		{"polled_events", "AWS/Lambda", "PolledEventCount", statSum, []dimension{{"EventSourceMappingUUID", rr.Mapping}}},
		{"archive_bytes", "AWS/Firehose", "IncomingBytes", statSum, archive},
		{"archive_records", "AWS/Firehose", "IncomingRecords", statSum, archive},
		{"archive_throttled", "AWS/Firehose", "ThrottledRecords", statSum, archive},
		{"archive_freshness_avg", "AWS/Firehose", "DeliveryToIceberg.DataFreshness", statAvg, archive},
		{"archive_freshness_max", "AWS/Firehose", "DeliveryToIceberg.DataFreshness", statMax, archive},
		{"breaker_opened", catalog.MetricNamespace, catalog.MetricBreakerOpened, statSum, store},
		{"row_too_large", catalog.MetricNamespace, catalog.MetricRowTooLarge, statSum, store},
	}
	for _, tb := range tables(rr) {
		dims := []dimension{{"TableName", tb.name}}
		qs = append(qs,
			metricQuery{tb.id + "_wcu", "AWS/DynamoDB", "ConsumedWriteCapacityUnits", statSum, dims},
			metricQuery{tb.id + "_rcu", "AWS/DynamoDB", "ConsumedReadCapacityUnits", statSum, dims},
			metricQuery{tb.id + "_write_throttles", "AWS/DynamoDB", "WriteThrottleEvents", statSum, dims},
			metricQuery{tb.id + "_read_throttles", "AWS/DynamoDB", "ReadThrottleEvents", statSum, dims})
		if tb.view {
			qs = append(qs, metricQuery{tb.id + "_conditional_failed", "AWS/DynamoDB", "ConditionalCheckFailedRequests", statSum, dims})
		}
	}
	for _, source := range []string{"Customer", "GlobalTable"} {
		qs = append(qs, metricQuery{"stream_wcu_" + strings.ToLower(source), "AWS/DynamoDB", "ConsumedWriteCapacityUnits", statSum,
			[]dimension{{"TableName", rr.StreamTable}, {"Source", source}}})
	}
	for _, other := range res.Regions {
		if other == rr.Name {
			continue
		}
		dims := []dimension{{"TableName", rr.StreamTable}, {"ReceivingRegion", other}}
		id := replicationID(other)
		qs = append(qs,
			metricQuery{id + "_avg", "AWS/DynamoDB", "ReplicationLatency", statAvg, dims},
			metricQuery{id + "_max", "AWS/DynamoDB", "ReplicationLatency", statMax, dims})
	}
	return qs
}

type table struct {
	id, name string
	view     bool
}

// tables returns the region's stream, meta and view tables, the views in name order.
func tables(rr *regionResult) []table {
	ts := []table{{id: "stream", name: rr.StreamTable}, {id: "meta", name: rr.MetaTable}}
	for _, v := range slices.Sorted(maps.Keys(rr.ViewTables)) {
		ts = append(ts, table{id: "view_" + v, name: rr.ViewTables[v], view: true})
	}
	return ts
}

// throughputReport renders the report of a run: per region, each line of explain's model
// for the measured workload next to what CloudWatch measured over the window, then the view
// lag and the client's side. metrics holds each region's statistics by query ID; a missing
// one had no datapoint. It returns the report and why the run failed, if it did.
func throughputReport(res *throughputResult, metrics map[string]map[string]float64,
	cfg *config.Config, cat *catalog.Catalog,
) (string, []string, error) {
	seconds := res.WindowEnd.Sub(res.WindowStart).Seconds()
	var events float64
	for _, rr := range res.PerRegion {
		events += float64(rr.Appended)
	}
	if seconds <= 0 || events == 0 {
		return "", nil, errors.New("the result has no measured window with appends")
	}
	w := explain.DefaultWorkload()
	w.Rate = events / seconds
	pages, tags := float64(res.Workload.Pages), float64(res.Workload.Tags)
	w.Rows = map[string]float64{"page_daily": pages, "tag_hourly": tags, "e2e_ratio": pages}
	w.Pass = map[string]float64{"tag_hourly": res.TagsPerEvent}
	w.UniqNew = map[string]float64{"page_daily": res.UniqNew}
	store, stream, err := explain.Inputs(cfg, cat, w, "page_views")
	if err != nil {
		return "", nil, err
	}
	model, err := explain.Estimate(store, stream)
	if err != nil {
		return "", nil, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "throughput run on %s, %s to %s (%s measured, after %s of warm-up)\n", res.Target,
		res.WindowStart.Format(time.RFC3339), res.WindowEnd.Format(time.RFC3339),
		res.WindowEnd.Sub(res.WindowStart), res.WindowStart.Sub(res.Start))
	if res.Phase != "done" {
		fmt.Fprintf(&b, "the run stopped while %s: what follows is what it recorded\n", res.Phase)
	}
	fmt.Fprintf(&b, "rate: %s events/s of a target of %s, all regions together\n", num(w.Rate), num(res.Workload.Rate))
	fmt.Fprintf(&b, "explain: dynaflux explain page_views -rate %s -rows page_daily=%s -rows tag_hourly=%s -rows e2e_ratio=%s -pass tag_hourly=%s -uniq-new page_daily=%s\n",
		num(w.Rate), num(pages), num(tags), num(pages), num(res.TagsPerEvent), num(res.UniqNew))
	var failures []string
	for i := range res.PerRegion {
		rr := &res.PerRegion[i]
		r := regionReport{
			rr: rr, m: metrics[rr.Name], regions: res.Regions, seconds: seconds, events: events,
			store: store, stream: stream, model: model,
		}
		if r.atShards, err = r.estimateAtShards(); err != nil {
			return "", nil, err
		}
		r.write(&b)
		failures = append(failures, r.failures()...)
	}
	writeLag(&b, res)
	writeClients(&b, res, seconds)
	failures = append(slices.Clone(res.Failures), failures...)
	b.WriteString("\nverdict: ")
	if len(failures) == 0 {
		b.WriteString("passed\n")
	}
	for i, f := range failures {
		if i > 0 {
			b.WriteString("         ")
		}
		b.WriteString(f + "\n")
	}
	return b.String(), failures, nil
}

// regionReport compares one region with explain's model.
type regionReport struct {
	rr              *regionResult
	m               map[string]float64
	regions         []string
	seconds, events float64
	store           explain.Store
	stream          explain.Stream
	model           explain.StreamCost
	// atShards is the model at the region's measured shard count.
	atShards explain.StreamCost
}

func (r *regionReport) estimateAtShards() (explain.StreamCost, error) {
	s := r.stream
	s.Partitions = r.rr.Shards
	return explain.Estimate(r.store, s)
}

// value is a measurement, or none when CloudWatch had no datapoint.
type value struct {
	v  float64
	ok bool
}

func (r *regionReport) get(id string) value {
	v, ok := r.m[id]
	return value{v, ok}
}

// per divides a measurement by d.
func (v value) per(d float64) value { return value{v.v / d, v.ok} }

func (r *regionReport) write(b *strings.Builder) {
	rr, model, at := r.rr, &r.model, &r.atShards
	rate := r.events / r.seconds
	g := grid{{rr.Name, "explain", fmt.Sprintf("at %d shards", rr.Shards), "measured", "ratio"}}
	line := func(label string, predicted, atShards float64, measured value) {
		ratio := ""
		if measured.ok && atShards > 0 {
			ratio = num(measured.v / atShards)
		}
		g.add(label, num(predicted), num(atShards), measured.text(), ratio)
	}
	only := func(label string, measured value) { g.add(label, "", "", measured.text()) }
	line("stream shards", float64(model.Partitions), float64(at.Partitions), value{float64(rr.Shards), true})
	invocations, polled := r.get("invocations"), r.get("polled_events")
	line("invocations/s", model.Invocations, at.Invocations, invocations.per(r.seconds))
	line("events per invocation", model.Batch, at.Batch, value{polled.v / invocations.v, polled.ok && invocations.ok && invocations.v > 0})
	line("duration ms, average", model.InvocationSeconds*msPerSecond, at.InvocationSeconds*msPerSecond, r.get("duration_avg"))
	only("duration ms, p50", r.get("duration_p50"))
	only("duration ms, p99", r.get("duration_p99"))
	only("duration ms, max", r.get("duration_max"))
	line("concurrency, max", model.Invocations*model.InvocationSeconds, at.Invocations*at.InvocationSeconds, r.get("concurrency_max"))
	only("iterator age ms, average", r.get("iterator_age_avg"))
	only("iterator age ms, max", r.get("iterator_age_max"))
	g.add("units per event of all regions")
	local, replicated := r.get("stream_wcu_customer"), r.get("stream_wcu_globaltable")
	line("  stream table writes", model.EventWrites/rate, at.EventWrites/rate,
		value{(local.v + replicated.v) / r.events, local.ok || replicated.ok})
	only("    local (Source=Customer)", local.per(r.events))
	only("    replicated (Source=GlobalTable)", replicated.per(r.events))
	only("    by TableName alone", r.get("stream_wcu").per(r.events))
	line("  meta table writes", model.ProgressWrites/rate, at.ProgressWrites/rate, r.get("meta_wcu").per(r.events))
	for i, v := range model.Views {
		id, a := "view_"+v.Name, &at.Views[i]
		line("  "+v.Name+" writes", (v.Rows.Write+v.Markers.Write)/rate, (a.Rows.Write+a.Markers.Write)/rate, r.get(id+"_wcu").per(r.events))
		line("  "+v.Name+" reads", (v.Rows.Read+v.Markers.Read)/rate, (a.Rows.Read+a.Markers.Read)/rate, r.get(id+"_rcu").per(r.events))
		line("  "+v.Name+" failed conditional writes", v.Conflicts/rate, a.Conflicts/rate, r.get(id+"_conditional_failed").per(r.events))
	}
	r.addDollars(&g, line)
	only("throttled requests and invocations", value{r.throttles(), true})
	for _, other := range r.regions {
		if other != rr.Name {
			only("replication ms to "+other+", average", r.get(replicationID(other)+"_avg"))
			only("replication ms to "+other+", max", r.get(replicationID(other)+"_max"))
		}
	}
	bytes, records := r.get("archive_bytes"), r.get("archive_records")
	only("archive records per event", records.per(r.events))
	only("archive bytes per record", value{bytes.v / records.v, bytes.ok && records.ok && records.v > 0})
	only("archive throttled records", r.get("archive_throttled"))
	only("archive freshness s, average", r.get("archive_freshness_avg"))
	only("archive freshness s, max", r.get("archive_freshness_max"))
	b.WriteString("\n")
	g.write(b)
}

// addDollars adds the monthly cost of each table's requests at explain's prices: the
// model's units, and the measured ones.
func (r *regionReport) addDollars(g *grid, line func(string, float64, float64, value)) {
	model, at := &r.model, &r.atShards
	g.add("$/month of requests at explain's prices")
	measured := func(c explain.Capacity, w, rd value) value {
		return value{c.Monthly(explain.Units{Write: w.v / r.seconds, Read: rd.v / r.seconds}), w.ok}
	}
	streamWrites := func(c *explain.StreamCost) float64 {
		return r.stream.Capacity.Monthly(explain.Units{Write: c.EventWrites})
	}
	local, replicated := r.get("stream_wcu_customer"), r.get("stream_wcu_globaltable")
	stream := measured(r.stream.Capacity, value{local.v + replicated.v, local.ok || replicated.ok}, r.get("stream_rcu"))
	meta := measured(r.store.Meta, r.get("meta_wcu"), r.get("meta_rcu"))
	line("  stream table, without TTL deletes", streamWrites(model), streamWrites(at), stream)
	line("  meta table", model.MetaDollars, at.MetaDollars, meta)
	total := [3]value{{streamWrites(model) + model.MetaDollars, true}, {streamWrites(at) + at.MetaDollars, true}, {stream.v + meta.v, stream.ok && meta.ok}}
	for i, v := range model.Views {
		m := measured(r.stream.Views[i].Capacity, r.get("view_"+v.Name+"_wcu"), r.get("view_"+v.Name+"_rcu"))
		line("  "+v.Name, v.Dollars, at.Views[i].Dollars, m)
		total[0].v += v.Dollars
		total[1].v += at.Views[i].Dollars
		total[2] = value{total[2].v + m.v, total[2].ok && m.ok}
	}
	line("  total", total[0].v, total[1].v, total[2])
	ttl := func(c *explain.StreamCost) string {
		return num(r.stream.Capacity.Monthly(explain.Units{Write: c.TTLWrites}))
	}
	g.add("  TTL deletes, from 2 days on", ttl(model), ttl(at), "not measured")
	g.add("$/month of Lambda and Firehose", num(model.Monthly.Lambda+model.Monthly.Firehose), num(at.Monthly.Lambda+at.Monthly.Firehose),
		"bench-cost.sh")
}

// replicationID is the query ID prefix of the replication latency to a region.
func replicationID(region string) string {
	return "replication_" + strings.ReplaceAll(region, "-", "_")
}

// throttles adds every throttle a region's tables and projector counted.
func (r *regionReport) throttles() float64 {
	n := r.m["lambda_throttles"]
	for _, tb := range tables(r.rr) {
		n += r.m[tb.id+"_write_throttles"] + r.m[tb.id+"_read_throttles"]
	}
	return n
}

// failures returns what in a region's metrics fails the run.
func (r *regionReport) failures() []string {
	var out []string
	add := func(n float64, what string) {
		if n > 0 {
			out = append(out, fmt.Sprintf("%s: %s: %s", r.rr.Name, what, num(n)))
		}
	}
	add(r.throttles(), "throttled DynamoDB requests and projector invocations")
	add(r.m["lambda_errors"], "failed projector invocations")
	add(r.m["breaker_opened"], "broken view versions (BreakerOpened)")
	add(r.m["row_too_large"], "view rows over the item limit (RowTooLarge)")
	add(r.m["archive_throttled"], "archive records that Firehose throttled")
	return out
}

func (v value) text() string {
	if !v.ok {
		return "no data"
	}
	return num(v.v)
}

const msPerSecond = 1000

// writeLag writes how long the window's probes took to show, per pair of regions.
func writeLag(b *strings.Builder, res *throughputResult) {
	type pair struct{ origin, observer string }
	lags, missed := map[pair][]float64{}, map[pair]int{}
	for _, p := range res.Probes {
		if p.Sent.Before(res.WindowStart) || !p.Sent.Before(res.WindowEnd) {
			continue
		}
		k := pair{p.Origin, p.Observer}
		if p.Missed {
			missed[k]++
		} else {
			lags[k] = append(lags[k], p.LagMS)
		}
	}
	b.WriteString("\nview lag ms, from an append's return to its row in a region's view, ±200 ms\n")
	g := grid{{"", "n", "p50", "p90", "p99", "max", "missed"}}
	for _, origin := range res.Regions {
		for _, observer := range res.Regions {
			k := pair{origin, observer}
			p := newPercentiles(lags[k])
			g.add(origin+" to "+observer, strconv.Itoa(p.N), num(p.P50), num(p.P90), num(p.P99), num(p.Max), strconv.Itoa(missed[k]))
		}
	}
	g.write(b)
}

// writeClients writes what each region's load generator saw in the window.
func writeClients(b *strings.Builder, res *throughputResult, seconds float64) {
	b.WriteString("\nclients in the window; append latency ms, from each append's scheduled time\n")
	g := grid{{"", "events/s", "missed", "unknown", "n", "p50", "p90", "p99", "max"}}
	for _, rr := range res.PerRegion {
		l := rr.Latency
		g.add(rr.Name, num(float64(rr.Appended)/seconds), strconv.FormatInt(rr.Missed, 10), strconv.FormatInt(rr.Unknown, 10),
			strconv.Itoa(l.N), num(l.P50), num(l.P90), num(l.P99), num(l.Max))
	}
	g.write(b)
	for _, rr := range res.PerRegion {
		for _, class := range slices.Sorted(maps.Keys(rr.Errors)) {
			fmt.Fprintf(b, "  %s: %d failed appends of class %s, such as: %s\n", rr.Name, rr.Errors[class], class, rr.Samples[class])
		}
	}
}

// grid is a table of the report, its first column aligned left and the others right.
type grid [][]string

func (g *grid) add(cells ...string) { *g = append(*g, cells) }

func (g *grid) write(b *strings.Builder) {
	var widths []int
	for _, row := range *g {
		for i, c := range row {
			if i == len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], utf8.RuneCountInString(c))
		}
	}
	for _, row := range *g {
		var line strings.Builder
		for i, c := range row {
			pad := strings.Repeat(" ", widths[i]-utf8.RuneCountInString(c))
			if i == 0 {
				line.WriteString("  " + c + pad)
			} else {
				line.WriteString("  " + pad + c)
			}
		}
		b.WriteString(strings.TrimRight(line.String(), " ") + "\n")
	}
}

// num formats x: whole numbers as they are, others to about 3 significant digits, and
// whole from 100 up.
func num(x float64) string {
	switch a := math.Abs(x); {
	case x == math.Trunc(x) || a >= 100:
		return strconv.FormatFloat(x, 'f', 0, 64)
	case a >= 10:
		return strconv.FormatFloat(x, 'f', 1, 64)
	case a >= 1:
		return strconv.FormatFloat(x, 'f', 2, 64)
	}
	return strconv.FormatFloat(x, 'g', 3, 64)
}

//nolint:gochecknoglobals // a test flag
var updateReport = flag.Bool("update", false, "rewrite testdata/throughput/report.txt from the fixture")

const fixtureDir = "testdata/throughput"

// TestThroughputReportOfFixture renders the report of a run of 2 regions at 1,000 events/s
// for 60 minutes, and compares it with report.txt. No run on AWS has been recorded yet, so
// the fixture's numbers are made up near explain's: replace them with the first run's
// result and metrics.
func TestThroughputReportOfFixture(t *testing.T) {
	t.Parallel()
	var res throughputResult
	readJSON(t, fixtureDir+"/result.json", &res)
	var metrics map[string]map[string]float64
	readJSON(t, fixtureDir+"/metrics.json", &metrics)
	cfg, cat := compile(t, e2eSQL, res.Regions)
	got, failures, err := throughputReport(&res, metrics, cfg, cat)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) > 0 {
		t.Errorf("failures %q, want none", failures)
	}
	golden := fixtureDir + "/report.txt"
	if *updateReport {
		if err = os.WriteFile(golden, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("the report differs from %s (rerun with -update and review the diff):\n%s", golden, got)
	}
}

// TestThroughputReportFails checks that what a run's metrics show of a failure is in the
// verdict.
func TestThroughputReportFails(t *testing.T) {
	t.Parallel()
	var res throughputResult
	readJSON(t, fixtureDir+"/result.json", &res)
	var metrics map[string]map[string]float64
	readJSON(t, fixtureDir+"/metrics.json", &metrics)
	res.Failures = []string{"us-east-1: page_daily of page /p1: 3 views, want 4"}
	metrics["us-west-2"]["view_tag_hourly_write_throttles"] = 12
	metrics["us-west-2"]["lambda_throttles"] = 1
	metrics["us-east-1"]["breaker_opened"] = 1
	cfg, cat := compile(t, e2eSQL, res.Regions)
	text, got, err := throughputReport(&res, metrics, cfg, cat)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"us-east-1: page_daily of page /p1: 3 views, want 4",
		"us-east-1: broken view versions (BreakerOpened): 1",
		"us-west-2: throttled DynamoDB requests and projector invocations: 13",
	}
	if !slices.Equal(got, want) {
		t.Errorf("failures\n%q\nwant\n%q", got, want)
	}
	if !strings.Contains(text, "verdict: "+want[0]+"\n") {
		t.Errorf("the verdict doesn't start with the run's own failure:\n%s", text)
	}
}

// TestThroughputMetricQueries checks that the fixture holds a value for every query, and
// nothing else: the report reads by query ID, so a renamed one would print "no data".
func TestThroughputMetricQueries(t *testing.T) {
	t.Parallel()
	var res throughputResult
	readJSON(t, fixtureDir+"/result.json", &res)
	var metrics map[string]map[string]float64
	readJSON(t, fixtureDir+"/metrics.json", &metrics)
	id := regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*$`)
	for i := range res.PerRegion {
		rr := &res.PerRegion[i]
		var ids []string
		for _, q := range metricQueries(&res, rr) {
			if !id.MatchString(q.ID) {
				t.Errorf("query ID %q isn't one GetMetricData takes", q.ID)
			}
			ids = append(ids, q.ID)
		}
		slices.Sort(ids)
		var have []string
		for k := range metrics[rr.Name] {
			have = append(have, k)
		}
		slices.Sort(have)
		if !slices.Equal(ids, have) {
			t.Errorf("%s: queries\n%q\nfixture\n%q", rr.Name, ids, have)
		}
	}
}

func readJSON(t *testing.T, name string, v any) {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}
