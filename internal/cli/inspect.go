// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/base64"
	"flag"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

// defaultLimit is how many events or rows inspect prints without -limit.
const defaultLimit = 100

func inspectCommand() command {
	return command{
		name:    "inspect",
		args:    "[flags] <stream|view|view@vN>",
		summary: "read a stream's events or a view's rows in the deployed store",
		doc: `Inspect reads a stream or a view with the deployed catalog of one region. -key gives
the value of a key column as column=value. ` + valueForms + `
For a stream, give every PRIMARY KEY column: inspect prints its events in
(event_time, event_id) order, within -from and -to when given. Events past the
stream's LOG RETENTION are only in its archive; inspect then prints the Athena SQL
that reads them.

For a view, give every ORDER BY column to read one row, or only the PRIMARY KEY
columns to read the partition. A view name reads its current version in the deployed
catalog, and view@vN another version that isn't retired. -raw prints the items as
DynamoDB stores them: each SPLIT item, the row version _v, the high-water marks _hwm
decoded, and the item's size against DynamoDB's 400 KB limit.

-json prints a stream's events as JSON Lines, and a view's rows as one JSON document.

Exit status: 0 when the events or rows are printed, also none; 1 when one view row
is asked for and doesn't exist, or the version is retired; and 2 for usage and I/O
problems, AWS access, and a name the deployed catalog doesn't define.
`,
		setup: func(fs *flag.FlagSet) func(*cli, []string) int {
			o := inspectOptions{aws: addAWSFlags(fs)}
			fs.Var(&o.keys, "key", "a key column's value, as `column=value`; repeat it for each one")
			fs.StringVar(&o.from, "from", "", "stream: read events from this event_time on, in RFC 3339 (`time`)")
			fs.StringVar(&o.to, "to", "", "stream: read events before this event_time, in RFC 3339 (`time`)")
			fs.IntVar(&o.limit, "limit", defaultLimit, "print at most `n` events or rows, or every one with 0")
			fs.BoolVar(&o.raw, "raw", false, "view: print the items as stored")
			fs.BoolVar(&o.json, "json", false, "print JSON")
			return func(c *cli, args []string) int { return c.inspect(o, args) }
		},
	}
}

type inspectOptions struct {
	aws      *awsFlags
	keys     pairs
	from, to string
	limit    int
	raw      bool
	json     bool
}

func (c *cli) inspect(o inspectOptions, args []string) int {
	switch {
	case len(args) != 1:
		return c.usageError("inspect", "name one stream or view")
	case o.limit < 0:
		return c.usageError("inspect", "-limit must be 0 or more")
	case o.raw && o.json:
		return c.usageError("inspect", "-raw prints text; leave out -json")
	}
	d, status := c.openDeployment("inspect", o.aws)
	if status != exitOK {
		return status
	}
	def := d.compiled.Catalog()
	if s, ok := def.Stream(args[0]); ok {
		if o.raw {
			return c.usageError("inspect", "-raw applies to views")
		}
		return c.inspectStream(d, s, o)
	}
	if o.from != "" || o.to != "" {
		return c.usageError("inspect", "-from and -to apply to streams")
	}
	return c.inspectView(d, args[0], o)
}

func (c *cli) inspectStream(d *deployment, s *catalog.Stream, o inspectOptions) int {
	types := map[string]catalog.Type{}
	for _, name := range s.PrimaryKey {
		col, _ := s.Column(name)
		types[name] = col.Type
	}
	vals, err := parsePairs("-key", "PRIMARY KEY column", o.keys, types)
	if err == nil && len(vals) < len(s.PrimaryKey) {
		err = fmt.Errorf("give every PRIMARY KEY column of %s with -key: %s", s.Name, strings.Join(s.PrimaryKey, ", "))
	}
	var from, to *int64
	if err == nil {
		from, err = eventTimeFlag("-from", o.from)
	}
	if err == nil {
		to, err = eventTimeFlag("-to", o.to)
	}
	if err != nil {
		return c.usageError("inspect", err.Error())
	}
	key := make([]expr.Value, len(s.PrimaryKey))
	for i, name := range s.PrimaryKey {
		key[i] = vals[name]
	}
	// Events hold the declared columns, then the implicit ones; inspect shows the implicit
	// ones first.
	cols := streamColumns(s)
	n := len(s.Columns)
	var events [][]expr.Value
	for ev, rerr := range d.engine.Read(c.ctx, d.compiled, s.Name, key, from, to) {
		if rerr != nil {
			c.errorf("dynaflux inspect: %s: %s", d.region, engineError(rerr))
			return exitUsage
		}
		if o.limit > 0 && len(events) == o.limit {
			c.errorf("note: stopped after %d events; -limit 0 prints them all", o.limit)
			break
		}
		events = append(events, slices.Concat(ev[n:], ev[:n]))
	}
	if o.json {
		writeJSONLines(&c.out, cols, events)
	} else {
		writeTable(&c.out, cols, events)
	}
	c.archiveNote(d, s, key, from, to)
	return exitOK
}

// streamColumns returns the columns that inspect shows for an event of s.
func streamColumns(s *catalog.Stream) []column {
	var cols []column
	for _, col := range slices.Concat(catalog.ImplicitColumns(), s.Columns) {
		cols = append(cols, column{col.Name, col.Type})
	}
	return cols
}

// eventTimeFlag parses the event_time of -from or -to; nil when it isn't set.
func eventTimeFlag(name, s string) (*int64, error) {
	if s == "" {
		return nil, nil //nolint:nilnil // an unset bound is open
	}
	v, err := valuetext.ParseText(catalog.EventTimeType(), s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return new(v.AsMicros()), nil
}

// archiveNote tells where the events are that a read of [from, to) may miss because they
// passed the stream's LOG RETENTION: in the region's archive, or nowhere.
func (c *cli) archiveNote(d *deployment, s *catalog.Stream, key []expr.Value, from, to *int64) {
	p, ok := s.LogRetention.Period()
	if !ok {
		return
	}
	cutoff, err := expr.AddInterval(time.Now().UnixMicro(), catalog.Interval{N: -p.N, Unit: p.Unit})
	if err != nil || from != nil && *from >= cutoff {
		return
	}
	window := fmt.Sprintf("note: events that arrived before %s are past LOG RETENTION %s",
		time.UnixMicro(cutoff).UTC().Format(time.RFC3339), p)
	if !archive.Archived(s, d.region) {
		c.errorf("%s, and %s isn't archived in %s", window, s.Name, d.region)
		return
	}
	var terms []string
	for i, name := range s.PrimaryKey {
		col, _ := s.Column(name)
		terms = append(terms, athenaName(name)+" = "+athenaLiteral(col.Type, key[i]))
	}
	if from != nil {
		terms = append(terms, athenaName(catalog.ColumnEventTime)+" >= "+athenaTime(*from))
	}
	if to != nil {
		terms = append(terms, athenaName(catalog.ColumnEventTime)+" < "+athenaTime(*to))
	}
	c.errorf("%s; the archive has them, in Athena:\n  SELECT * FROM %s.%s WHERE %s ORDER BY %s, %s", window,
		athenaName(d.naming.ArchiveDatabase()), athenaName(catalog.DedupView(catalog.IcebergTable(s.Name))),
		strings.Join(terms, " AND "), athenaName(catalog.ColumnEventTime), athenaName(catalog.ColumnEventID))
}

func athenaName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// athenaLiteral writes a key value as an Athena literal of its archive column's type.
func athenaLiteral(t catalog.Type, v expr.Value) string {
	text := string(valuetext.AppendText(nil, t, v))
	switch t.Kind {
	case catalog.KindString, catalog.KindUUID:
		if strings.ContainsFunc(text, unicode.IsControl) {
			var b strings.Builder
			for _, r := range text {
				fmt.Fprintf(&b, `\+%06X`, r)
			}
			return "U&'" + b.String() + "'"
		}
		return "'" + strings.ReplaceAll(text, "'", "''") + "'"
	case catalog.KindDate:
		return "DATE '" + text + "'"
	case catalog.KindDateTime, catalog.KindDateTime64:
		return athenaTime(v.AsMicros())
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindUInt64, catalog.KindInt8,
		catalog.KindInt16, catalog.KindInt32, catalog.KindInt64, catalog.KindFloat64, catalog.KindDecimal, catalog.KindBool:
	}
	return text
}

func athenaTime(us int64) string {
	return "TIMESTAMP '" + time.UnixMicro(us).UTC().Format("2006-01-02 15:04:05.000000") + "'"
}

func (c *cli) inspectView(d *deployment, name string, o inspectOptions) int {
	id, vv, state, status := c.viewVersion(d, name)
	if status != exitOK {
		return status
	}
	key, err := viewKey(vv, o.keys)
	if err != nil {
		return c.usageError("inspect", err.Error())
	}
	c.stateNote(d.region, state)
	if o.raw {
		return c.inspectRaw(d, id, vv, key, o.limit)
	}
	var rows [][]expr.Value
	if len(key) == len(vv.Keys) {
		rows, status = c.viewRow(d, id, key)
	} else {
		rows, status = c.viewPartition(d, id, key, o.limit)
	}
	if status != exitOK {
		return status
	}
	if o.json {
		writeViewJSON(&c.out, d.region, state, viewColumns(vv), rows)
	} else {
		writeTable(&c.out, viewColumns(vv), rows)
	}
	return exitOK
}

// viewVersion finds the version that name names, a view's current one or view@vN, and
// its state in the region; a retired version is refused.
func (c *cli) viewVersion(d *deployment, name string) (string, *catalog.ViewVersion, meta.ViewState, int) {
	def := d.compiled.Catalog()
	id := name
	if v, ok := def.View(name); ok {
		vv, cur := v.Current()
		if !cur {
			ids := make([]string, len(v.Versions))
			for i, vv := range v.Versions {
				ids[i] = catalog.VersionID(v.Name, vv.Number)
			}
			return "", nil, meta.ViewState{}, c.usageError("inspect", fmt.Sprintf("%s has no current version in the deployed catalog of %s; name one: %s",
				name, d.region, strings.Join(ids, ", ")))
		}
		id = catalog.VersionID(v.Name, vv.Number)
	}
	_, vv, ok := d.compiled.Projector().Version(id)
	if !ok && !slices.Contains(def.Retired, id) {
		return "", nil, meta.ViewState{}, c.usageError("inspect", notDeployed("stream or view", name, d.region, deployedNames(def)))
	}
	states, err := d.meta.ViewStates(c.ctx)
	if err != nil {
		c.errorf("dynaflux inspect: %s: read the view states: %v", d.region, err)
		return "", nil, meta.ViewState{}, exitUsage
	}
	state, found := states[id]
	if !found {
		state = meta.ViewState{Version: id, State: meta.PendingBackfill}
	}
	if !ok || state.State == meta.Retired {
		c.errorf("dynaflux inspect: %s is retired in %s", id, d.region)
		return "", nil, meta.ViewState{}, exitErrors
	}
	return id, vv, state, exitOK
}

func (c *cli) viewRow(d *deployment, id string, key []expr.Value) ([][]expr.Value, int) {
	row, found, err := d.engine.ReadRow(c.ctx, d.compiled, id, key)
	if err != nil {
		c.errorf("dynaflux inspect: %s: %s", d.region, engineError(err))
		return nil, exitUsage
	}
	if !found {
		c.errorf("dynaflux inspect: %s has no row with that key in %s", id, d.region)
		return nil, exitErrors
	}
	return [][]expr.Value{row}, exitOK
}

func (c *cli) viewPartition(d *deployment, id string, key []expr.Value, limit int) ([][]expr.Value, int) {
	var rows [][]expr.Value
	for row, err := range d.engine.ReadPartition(c.ctx, d.compiled, id, key) {
		if err != nil {
			c.errorf("dynaflux inspect: %s: %s", d.region, engineError(err))
			return nil, exitUsage
		}
		if len(rows) == limit && limit > 0 {
			c.errorf("note: stopped after %d rows; -limit 0 prints them all", limit)
			break
		}
		rows = append(rows, row)
	}
	return rows, exitOK
}

func deployedNames(def *catalog.Catalog) []string {
	var names []string
	for _, s := range def.Streams {
		names = append(names, s.Name)
	}
	for _, v := range def.Views {
		names = append(names, v.Name)
		for _, vv := range v.Versions {
			names = append(names, catalog.VersionID(v.Name, vv.Number))
		}
	}
	return names
}

// stateNote says on stderr what a version's state means for the rows inspect prints.
func (c *cli) stateNote(region string, s meta.ViewState) {
	switch s.State {
	case meta.Live:
		if !s.PartialSince.IsZero() {
			c.errorf("note: %s is live in %s, with the events that arrived from %s on", s.Version, region,
				s.PartialSince.UTC().Format(time.RFC3339))
		}
	case meta.Broken:
		msg := ""
		if s.Failure != nil {
			msg = ": " + s.Failure.Error
		}
		c.errorf("note: %s is broken in %s, so its rows miss every event since it broke%s", s.Version, region, msg)
	case meta.PendingBackfill, meta.Activating, meta.Retired:
		c.errorf("note: %s is %s in %s, so its rows may miss events", s.Version, s.State, region)
	}
}

// viewKey returns the values of vv's keys in -key: every ORDER BY column, or only the
// PRIMARY KEY columns.
func viewKey(vv *catalog.ViewVersion, given pairs) ([]expr.Value, error) {
	types := map[string]catalog.Type{}
	names := make([]string, len(vv.Keys))
	for i, k := range vv.Keys {
		types[k.Name] = k.Expr.Type
		names[i] = k.Name
	}
	vals, err := parsePairs("-key", "ORDER BY column", given, types)
	if err != nil {
		return nil, err
	}
	n := len(vals)
	if n != vv.PartitionKeyLen && n != len(vv.Keys) || slices.ContainsFunc(names[:n], func(k string) bool {
		_, ok := vals[k]
		return !ok
	}) {
		return nil, fmt.Errorf("give the PRIMARY KEY columns with -key (%s) to read a partition, or every ORDER BY column (%s) to read a row",
			strings.Join(names[:vv.PartitionKeyLen], ", "), strings.Join(names, ", "))
	}
	key := make([]expr.Value, n)
	for i := range key {
		key[i] = vals[names[i]]
	}
	return key, nil
}

// viewColumns returns a row's columns: the keys in ORDER BY order, then the aggregates.
func viewColumns(vv *catalog.ViewVersion) []column {
	cols := make([]column, 0, len(vv.Keys)+len(vv.Aggregates))
	for _, k := range vv.Keys {
		cols = append(cols, column{k.Name, k.Expr.Type})
	}
	for _, a := range vv.Aggregates {
		cols = append(cols, column{a.Name, a.Type})
	}
	return cols
}

// writeViewJSON writes the rows of a view version as one JSON document.
func writeViewJSON(b *strings.Builder, region string, s meta.ViewState, cols []column, rows [][]expr.Value) {
	str := catalog.Type{Kind: catalog.KindString}
	text := func(v string) string { return string(valuetext.AppendJSON(nil, str, expr.String(v))) }
	fmt.Fprintf(b, `{"format":1,"region":%s,"version":%s,"state":%s`, text(region), text(s.Version), text(string(s.State)))
	if !s.PartialSince.IsZero() {
		fmt.Fprintf(b, `,"partial_since":%s`, text(s.PartialSince.UTC().Format(time.RFC3339Nano)))
	}
	var lines strings.Builder
	writeJSONLines(&lines, cols, rows)
	b.WriteString(`,"rows":[` + strings.ReplaceAll(strings.TrimSuffix(lines.String(), "\n"), "\n", ",") + "]}\n")
}

// inspectRaw prints the items that hold the row or partition key of version id as stored.
func (c *cli) inspectRaw(d *deployment, id string, vv *catalog.ViewVersion, key []expr.Value, limit int) int {
	types := make([]catalog.Type, len(vv.Keys))
	for i, k := range vv.Keys {
		types[i] = k.Expr.Type
	}
	n := vv.PartitionKeyLen
	pk, err := keys.Key(types[:n], key[:n])
	q := store.Query{PK: pk}
	if err == nil && len(key) > n {
		var lsk string
		if lsk, err = keys.Key(types[n:], key[n:]); err == nil {
			q.From, q.To = lsk, lsk
			if vv.Split > 1 {
				q.From, q.To = keys.SplitKey(lsk, 0), keys.SplitKey(lsk, vv.Split-1)
			}
		}
	}
	if err != nil {
		return c.usageError("inspect", err.Error())
	}
	if limit > 0 {
		q.Limit = limit + 1
	}
	view, _, _ := strings.Cut(id, "@v")
	count := 0
	for it, qerr := range d.clients.data.Query(c.ctx, d.naming.ViewTable(view, vv.Number), q) {
		if qerr != nil {
			c.errorf("dynaflux inspect: %s: %v", d.region, qerr)
			return exitUsage
		}
		if count == limit && limit > 0 {
			c.errorf("note: stopped after %d items; -limit 0 prints them all", limit)
			break
		}
		count++
		c.writeItem(it)
	}
	if count == 0 && len(key) == len(vv.Keys) {
		c.errorf("dynaflux inspect: %s has no item with that key in %s", id, d.region)
		return exitErrors
	}
	return exitOK
}

// writeItem prints a view item as stored: its key and size, then its attributes by name,
// with _hwm decoded.
func (c *cli) writeItem(it store.Item) {
	fmt.Fprintf(&c.out, "item pk %s sk %s: %d of DynamoDB's %d bytes\n",
		strconv.Quote(it[catalog.AttrPK].Str), strconv.Quote(it[catalog.AttrSK].Str), store.ItemSize(it), store.MaxItemSize)
	var lines [][]string
	for _, name := range slices.Sorted(maps.Keys(it)) {
		a := it[name]
		switch name {
		case catalog.AttrPK, catalog.AttrSK:
		case catalog.AttrHighWaterMarks:
			lines = append(lines, hwmLines(a)...)
		default:
			lines = append(lines, []string{"  " + name, attrText(a)})
		}
	}
	alignColumns(&c.out, lines)
}

// hwmLines decodes _hwm: each unit key with its sequence number, and the backfill's mark.
func hwmLines(a store.Value) [][]string {
	h, err := keys.DecodeHWM(a, true)
	if err != nil {
		return [][]string{{"  " + catalog.AttrHighWaterMarks, attrText(a) + " (" + err.Error() + ")"}}
	}
	var lines [][]string
	label := "  " + catalog.AttrHighWaterMarks
	for _, uk := range slices.Sorted(maps.Keys(h.Units)) {
		lines = append(lines, []string{label, "unit " + uk + " seq " + h.Units[uk]})
		label = ""
	}
	if h.Backfill != nil {
		lines = append(lines, []string{label, "backfill unit " + h.Backfill.UnitKey + " seq " + h.Backfill.Seq})
	}
	return lines
}

// attrText writes a DynamoDB attribute value with its type.
func attrText(a store.Value) string {
	switch a.Type {
	case store.S:
		return "S " + strconv.Quote(a.Str)
	case store.N:
		return "N " + a.Str
	case store.B:
		return "B " + base64.StdEncoding.EncodeToString(a.Bin)
	case store.BOOL:
		return "BOOL " + strconv.FormatBool(a.Bool)
	case store.L:
		elems := make([]string, len(a.List))
		for i, e := range a.List {
			elems[i] = attrText(e)
		}
		return "L [" + strings.Join(elems, ", ") + "]"
	case store.M:
		names := slices.Sorted(maps.Keys(a.Map))
		for i, k := range names {
			names[i] = strconv.Quote(k) + ": " + attrText(a.Map[k])
		}
		return "M {" + strings.Join(names, ", ") + "}"
	}
	return "a type Dynaflux doesn't write"
}
