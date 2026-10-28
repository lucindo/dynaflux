// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfilltest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// Athena runs in memory the two statements of package archive that a backfill sends: the
// partitions query and the UNLOAD of a range of event times. It knows each by its exact
// text, never by parsing SQL, and answers StartQueryExecution, GetQueryExecution,
// GetQueryResults and StopQueryExecution in Athena's JSON protocol. A query runs when it
// starts, is RUNNING at its first poll, and ends at the next. An UNLOAD writes the rows of
// its range as gzip JSON files, in the forms of the statement's projection, under its
// destination, and its data manifest under athena/backfill/; it fails on a destination that
// holds an object, as Athena does. It is safe for concurrent use.
type Athena struct {
	s3      *S3
	bucket  string
	db      string
	cat     *catalog.Catalog
	tables  map[string]archive.Table
	records func(stream string) [][]byte

	mu      sync.Mutex
	fail    func(dest string) bool
	tokens  map[string]string // ClientRequestToken → query ID
	queries map[string]*query
}

// NewAthena returns an Athena whose archive database db holds the tables of cat's streams
// that region archives, with the records that records returns of each: the lines Firehose
// received. It writes results to bucket in s3.
func NewAthena(s3 *S3, bucket, db string, cat *catalog.Catalog, region string, records func(stream string) [][]byte) *Athena {
	a := &Athena{s3: s3, bucket: bucket, db: db, cat: cat, tables: map[string]archive.Table{}, records: records,
		tokens: map[string]string{}, queries: map[string]*query{}}
	for _, t := range archive.SchemaFor(cat, region).Tables {
		a.tables[t.Name] = t
	}
	return a
}

// FailExports fails the UNLOADs whose destination fail returns true for, after each wrote
// one file, as a query that fails midway leaves its files.
func (a *Athena) FailExports(fail func(dest string) bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fail = fail
}

// Started returns the statements of the queries started so far, in no order.
func (a *Athena) Started() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, q := range a.queries {
		out = append(out, q.statement)
	}
	return out
}

// Athena's query states, and the error types the fake fails queries with.
const (
	stateRunning   = "RUNNING"
	stateSucceeded = "SUCCEEDED"
	stateFailed    = "FAILED"
	stateCancelled = "CANCELLED"
	errorUser      = 1100 // a statement Athena refuses to run, as on a destination that holds data
	errorInternal  = 100
	categoryUser   = 2
)

// query is a query's statement and where it ended.
type query struct {
	statement string
	polls     int
	state     string // SUCCEEDED, FAILED or CANCELLED once polled
	errorType int    // when FAILED
	message   string
	scanned   int64
	manifest  string     // a succeeded UNLOAD's data manifest
	rows      [][]string // a SELECT's, its columns' names first
}

type started struct {
	ID string `json:"QueryExecutionId"`
}

type athenaRequest struct {
	QueryString        string `json:"QueryString"`
	ClientRequestToken string `json:"ClientRequestToken"`
	QueryExecutionID   string `json:"QueryExecutionId"`
}

// ServeHTTP implements [http.Handler].
func (a *Athena) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in athenaRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		athenaError(w, "InvalidRequestException", err.Error())
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, op, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	if op == "StartQueryExecution" {
		a.start(w, in)
		return
	}
	q, ok := a.queries[in.QueryExecutionID]
	if !ok {
		athenaError(w, "InvalidRequestException", "QueryExecution "+in.QueryExecutionID+" was not found")
		return
	}
	switch op {
	case "GetQueryExecution":
		writeJSON(w, a.execution(in.QueryExecutionID, q))
	case "GetQueryResults":
		writeJSON(w, results(q))
	case "StopQueryExecution":
		if q.polls == 0 {
			q.state, q.polls = stateCancelled, 1
		}
		writeJSON(w, map[string]any{})
	default:
		athenaError(w, "InvalidRequestException", "the fake doesn't serve "+op)
	}
}

// start answers StartQueryExecution: the query of a token again, or a new query.
func (a *Athena) start(w http.ResponseWriter, in athenaRequest) {
	if id, ok := a.tokens[in.ClientRequestToken]; ok {
		if a.queries[id].statement != in.QueryString {
			athenaError(w, "InvalidRequestException", "Idempotent parameters do not match")
			return
		}
		writeJSON(w, started{id})
		return
	}
	id := fmt.Sprintf("q-%04d", len(a.queries))
	q, err := a.run(in.QueryString, id)
	if err != nil {
		athenaError(w, "InvalidRequestException", err.Error())
		return
	}
	a.queries[id], a.tokens[in.ClientRequestToken] = q, id
	writeJSON(w, started{id})
}

// The literals of an UNLOAD that archive.Unload writes, which the fake checks by writing
// the statement again from them.
var (
	timestampLiteral = regexp.MustCompile(`TIMESTAMP '([^']*)'`)
	toLiteral        = regexp.MustCompile(`\nTO '([^']*)'\n`)
)

const timestampLayout = "2006-01-02 15:04:05.000000"

// run runs statement as query id: one that archive.PartitionsQuery or archive.Unload writes
// for a stream of the catalog.
func (a *Athena) run(statement, id string) (*query, error) {
	for name := range a.tables {
		s, _ := a.cat.Stream(name)
		if statement == archive.PartitionsQuery(a.db, s) {
			return a.partitions(statement, name), nil
		}
	}
	times, to := timestampLiteral.FindAllStringSubmatch(statement, -1), toLiteral.FindStringSubmatch(statement)
	if len(times) == 2 && to != nil {
		from, ferr := time.Parse(timestampLayout, times[0][1])
		until, uerr := time.Parse(timestampLayout, times[1][1])
		for name := range a.tables {
			s, _ := a.cat.Stream(name)
			if ferr == nil && uerr == nil && statement == archive.Unload(a.db, s, from, until, to[1]) {
				return a.unload(statement, name, from, until, to[1], id)
			}
		}
	}
	return nil, fmt.Errorf("the fake runs only package archive's statements, not %q", statement)
}

// record is an archive record and its event time.
type record struct {
	line []byte
	at   time.Time
}

func (a *Athena) recordsOf(stream string) ([]record, error) {
	var out []record
	for _, line := range a.records(stream) {
		var cols struct {
			EventTime json.Number `json:"event_time"`
		}
		if err := json.Unmarshal(line, &cols); err != nil {
			return nil, fmt.Errorf("archive record %s: %w", line, err)
		}
		micros, err := cols.EventTime.Int64()
		if err != nil {
			return nil, fmt.Errorf("archive record %s: event_time: %w", line, err)
		}
		out = append(out, record{line: line, at: time.UnixMicro(micros).UTC()})
	}
	return out, nil
}

// partitions runs archive.PartitionsQuery: each day's record count, and its records' bytes
// in place of the size of its data files.
func (a *Athena) partitions(statement, stream string) *query {
	q := &query{statement: statement, state: stateSucceeded, rows: [][]string{{"day", "record_count", "total_size"}}}
	recs, err := a.recordsOf(stream)
	if err != nil {
		q.state, q.errorType, q.message = stateFailed, errorInternal, err.Error()
		return q
	}
	counts, sizes := map[string]int{}, map[string]int{}
	for _, r := range recs {
		day := r.at.Format(time.DateOnly)
		counts[day]++
		sizes[day] += len(r.line)
	}
	for _, day := range slices.Sorted(maps.Keys(counts)) {
		q.rows = append(q.rows, []string{day, strconv.Itoa(counts[day]), strconv.Itoa(sizes[day])})
	}
	return q
}

// unload runs archive.Unload of stream's records in [from, to) to dest, in two files, as
// query id.
func (a *Athena) unload(statement, stream string, from, to time.Time, dest, id string) (*query, error) {
	prefix, ok := strings.CutPrefix(dest, "s3://"+a.bucket+"/")
	if !ok {
		return nil, fmt.Errorf("the destination %s isn't in s3://%s", dest, a.bucket)
	}
	q := &query{statement: statement, state: stateSucceeded}
	if len(a.s3.Keys(a.bucket, prefix)) > 0 {
		q.state, q.errorType = stateFailed, errorUser
		q.message = "HIVE_PATH_ALREADY_EXISTS: Target directory for table already exists: " + dest
		return q, nil
	}
	recs, err := a.recordsOf(stream)
	if err != nil {
		return nil, err
	}
	var rows [][]byte
	for _, r := range recs {
		if r.at.Before(from) || !r.at.Before(to) {
			continue
		}
		row, perr := a.projection(stream, r.line)
		if perr != nil {
			return nil, perr
		}
		rows = append(rows, row)
		q.scanned += int64(len(r.line))
	}
	files := [][][]byte{rows[:len(rows)/2], rows[len(rows)/2:]}
	if a.fail != nil && a.fail(dest) {
		files = files[:1]
		q.state, q.errorType, q.message = stateFailed, errorInternal, "INTERNAL_ERROR_QUERY_ENGINE"
	}
	var manifest strings.Builder
	for i, file := range files {
		key := fmt.Sprintf("%s%s_%d.gz", prefix, id, i)
		a.s3.Put(a.bucket, key, gzipLines(file))
		manifest.WriteString("s3://" + a.bucket + "/" + key + "\n")
	}
	if q.state == stateSucceeded {
		q.manifest = "athena/backfill/" + id + "-manifest.csv"
		a.s3.Put(a.bucket, q.manifest, []byte(manifest.String()))
	}
	return q, nil
}

// projection returns line in the forms of archive.Unload's projection: Athena writes the
// bigint and timestamp columns cast to text, and the other columns as Firehose received them.
func (a *Athena) projection(stream string, line []byte) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(line))
	d.UseNumber()
	var row map[string]any
	if err := d.Decode(&row); err != nil {
		return nil, err
	}
	for _, c := range a.tables[stream].Columns {
		v, ok := row[c.Name]
		if !ok {
			continue
		}
		elem := strings.TrimSuffix(strings.TrimPrefix(c.Type, "array<"), ">")
		if c.Type == elem {
			row[c.Name] = text(elem, v)
			continue
		}
		if vs, isArray := v.([]any); isArray {
			for i := range vs {
				vs[i] = text(elem, vs[i])
			}
		}
	}
	return json.Marshal(row)
}

// text returns v of Athena type typ as CAST(v AS varchar) writes it, for bigint and
// timestamp; other values as they are.
func text(typ string, v any) any {
	n, ok := v.(json.Number)
	switch {
	case !ok:
		return v
	case typ == "bigint":
		return n.String()
	case typ == "timestamp":
		micros, err := n.Int64()
		if err != nil {
			return v
		}
		return time.UnixMicro(micros).UTC().Format(timestampLayout)
	}
	return v
}

func gzipLines(lines [][]byte) []byte {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	for _, l := range lines {
		zw.Write(append(l, '\n')) //nolint:errcheck,gosec // writes to a buffer don't fail
	}
	zw.Close() //nolint:gosec // as above
	return b.Bytes()
}

// execution answers GetQueryExecution.
func (a *Athena) execution(id string, q *query) any {
	status := map[string]any{"State": stateRunning}
	if q.polls > 0 {
		status["State"] = q.state
	}
	if q.polls > 0 && q.state == stateFailed {
		status["StateChangeReason"] = q.message
		status["AthenaError"] = map[string]any{"ErrorCategory": categoryUser, "ErrorType": q.errorType, "Retryable": false, "ErrorMessage": q.message}
	}
	q.polls++
	stats := map[string]any{"DataScannedInBytes": q.scanned}
	if q.manifest != "" {
		stats["DataManifestLocation"] = "s3://" + a.bucket + "/" + q.manifest
	}
	return map[string]any{"QueryExecution": map[string]any{
		"QueryExecutionId": id, "Query": q.statement, "StatementType": "DML", "Status": status, "Statistics": stats,
	}}
}

// results answers GetQueryResults with every row on one page.
func results(q *query) any {
	var rows, columns []any
	for i, row := range q.rows {
		var data []any
		for _, v := range row {
			data = append(data, map[string]string{"VarCharValue": v})
			if i == 0 {
				columns = append(columns, map[string]string{"Name": v, "Type": "varchar"})
			}
		}
		rows = append(rows, map[string]any{"Data": data})
	}
	return map[string]any{"ResultSet": map[string]any{"Rows": rows, "ResultSetMetadata": map[string]any{"ColumnInfo": columns}}, "UpdateCount": 0}
}

// writeJSON answers with v in the JSON 1.1 protocol.
func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.Write(data) //nolint:errcheck,gosec // the client sees a short body
}

// athenaError answers with an error of type code.
func athenaError(w http.ResponseWriter, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.Header().Set("X-Amzn-Errortype", code)
	w.WriteHeader(http.StatusBadRequest)
	data, err := json.Marshal(map[string]string{"__type": code, "Message": message})
	if err != nil {
		return
	}
	w.Write(data) //nolint:errcheck,gosec // the client sees a short body
}
