// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/examples/analytics/internal/flux"
	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// archived is what a region archived of some events: each event once, as the text of its
// columns with NULLs left out, and the stream record of every copy archived.
type archived struct {
	events []map[string]string
	copies []copyRecord
}

// copyRecord is the stream record of an archived copy of an event.
type copyRecord struct {
	event, shard, seq string
	arrival           time.Time
}

// Column text is what Athena shows: a timestamp as 2006-01-02 15:04:05.000000, an array as
// JSON. The suite compares only the column types of the e2e store.
const timestampLayout = "2006-01-02 15:04:05.000000"

func timestampText(t time.Time) string { return t.UTC().Format(timestampLayout) }

// tableColumns returns a stream's archive columns before the bookkeeping ones, in table
// order (docs/spec/infra.md §7.3).
func tableColumns(s *catalog.Stream) []catalog.Column {
	return slices.Concat(catalog.ImplicitColumns(), s.Columns)
}

// athenaText returns the Athena expression that gives a column's text.
func athenaText(c catalog.Column) (string, error) {
	name := `"` + c.Name + `"`
	switch {
	case c.Type.Array && c.Type.Kind == catalog.KindString:
		return "json_format(CAST(" + name + " AS JSON))", nil
	case c.Type.Array:
	case c.Type.Kind == catalog.KindString, c.Type.Kind == catalog.KindUUID:
		return name, nil
	case c.Type.Kind == catalog.KindUInt32, c.Type.Kind == catalog.KindDateTime64:
		return "CAST(" + name + " AS varchar)", nil
	}
	return "", fmt.Errorf("column %s: the e2e comparison has no text for %s", c.Name, c.Type)
}

// jsonText returns the text of a value in an archive record (internal/archive's encoding).
func jsonText(t catalog.Type, raw json.RawMessage) (string, error) {
	switch {
	case t.Array && t.Kind == catalog.KindString:
		var b bytes.Buffer
		err := json.Compact(&b, raw)
		return b.String(), err
	case t.Array:
	case t.Kind == catalog.KindString, t.Kind == catalog.KindUUID:
		var s string
		err := json.Unmarshal(raw, &s)
		return s, err
	case t.Kind == catalog.KindUInt32:
		n, err := strconv.ParseUint(string(raw), 10, 32)
		return strconv.FormatUint(n, 10), err
	case t.Kind == catalog.KindDateTime64:
		us, err := strconv.ParseInt(string(raw), 10, 64)
		return timestampText(time.UnixMicro(us)), err
	}
	return "", fmt.Errorf("the e2e comparison has no text for %s", t)
}

// fromRecords returns the archived events of stream among the archive records recs, in
// the order Firehose received them, whose column key starts with prefix. Of an event's
// copies, the first is the event, as in the dedup view.
func fromRecords(s *catalog.Stream, recs [][]byte, key, prefix string) (archived, error) {
	var a archived
	seen := map[[2]string]bool{}
	for _, rec := range recs {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rec, &fields); err != nil {
			return archived{}, fmt.Errorf("archive record %s: %w", rec, err)
		}
		ev := map[string]string{}
		for _, c := range tableColumns(s) {
			raw, ok := fields[c.Name]
			if !ok {
				continue
			}
			text, err := jsonText(c.Type, raw)
			if err != nil {
				return archived{}, fmt.Errorf("archive record %s: %s: %w", rec, c.Name, err)
			}
			ev[c.Name] = text
		}
		if !strings.HasPrefix(ev[key], prefix) {
			continue
		}
		cp, err := copyOf(ev[catalog.ColumnEventID], fields)
		if err != nil {
			return archived{}, fmt.Errorf("archive record %s: %w", rec, err)
		}
		a.copies = append(a.copies, cp)
		id := [2]string{ev[catalog.ColumnEventTime], ev[catalog.ColumnEventID]}
		if !seen[id] {
			seen[id] = true
			a.events = append(a.events, ev)
		}
	}
	return a, nil
}

// copyOf returns the stream record of an archive record of an event.
func copyOf(event string, fields map[string]json.RawMessage) (copyRecord, error) {
	cp := copyRecord{event: event}
	for name, dst := range map[string]*string{archive.ColumnShard: &cp.shard, archive.ColumnSeq: &cp.seq} {
		if raw, ok := fields[name]; ok {
			*dst = strings.Trim(string(raw), `"`)
		}
	}
	if raw, ok := fields[archive.ColumnArrival]; ok {
		us, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return copyRecord{}, fmt.Errorf("%s: %w", archive.ColumnArrival, err)
		}
		cp.arrival = time.UnixMicro(us)
	}
	return cp, nil
}

// pageViewText is the archive text of a page view as the region's stream holds it.
func pageViewText(ev flux.PageView) (map[string]string, error) {
	if ev.Tags == nil {
		ev.Tags = []string{} // [] rather than null
	}
	// No HTML escaping, as the archive encodes strings.
	var tags bytes.Buffer
	enc := json.NewEncoder(&tags)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(ev.Tags); err != nil {
		return nil, err
	}
	m := implicitText(ev.EventID, ev.EventTime, ev.IngestTime, ev.OriginRegion)
	m["user_id"], m["page"], m["tags"] = ev.UserID, ev.Page, strings.TrimSuffix(tags.String(), "\n")
	m["duration_ms"], m["country"] = strconv.FormatUint(uint64(ev.DurationMS), 10), ev.Country
	if ev.Referrer != nil {
		m["referrer"] = *ev.Referrer
	}
	return m, nil
}

// apiCallText is the archive text of an API call as the region's stream holds it.
func apiCallText(ev flux.APICall) map[string]string {
	m := implicitText(ev.EventID, ev.EventTime, ev.IngestTime, ev.OriginRegion)
	m["account_id"], m["endpoint"], m["request_id"] = ev.AccountID, ev.Endpoint, ev.RequestID.String()
	m["units"] = strconv.FormatUint(uint64(ev.Units), 10)
	return m
}

func implicitText(id string, eventTime, ingestTime time.Time, origin string) map[string]string {
	return map[string]string{
		catalog.ColumnEventID: id, catalog.ColumnEventTime: timestampText(eventTime),
		catalog.ColumnIngestTime: timestampText(ingestTime), catalog.ColumnOriginRegion: origin,
	}
}

// byEventID sorts events for a comparison.
func byEventID(evs []map[string]string) []map[string]string {
	return slices.SortedFunc(slices.Values(evs), func(a, b map[string]string) int {
		return strings.Compare(a[catalog.ColumnEventID], b[catalog.ColumnEventID])
	})
}
