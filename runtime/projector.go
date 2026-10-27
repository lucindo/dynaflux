// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// windowEvent is the part of a DynamoDB stream event with a tumbling window (ADR 0003) that
// the projector reads. Fields it doesn't declare can't fail decoding, which would block
// the shard for good.
type windowEvent struct {
	Records        []streamRecord `json:"Records"`
	ShardID        string         `json:"shardId"`
	EventSourceARN string         `json:"eventSourceARN"`
}

type streamRecord struct {
	EventName string       `json:"eventName"`
	Change    streamChange `json:"dynamodb"`
}

type streamChange struct {
	ApproximateCreationDateTime json.Number     `json:"ApproximateCreationDateTime"`
	SequenceNumber              string          `json:"SequenceNumber"`
	StreamViewType              string          `json:"StreamViewType"`
	NewImage                    map[string]attr `json:"NewImage"`
	OldImage                    map[string]attr `json:"OldImage"`
}

// streamView is the only view type whose records carry the images the core reads.
const streamView = "NEW_AND_OLD_IMAGES"

// attr is an attribute value in the stream's JSON. N keeps the text DynamoDB sent.
type attr struct {
	S    *string         `json:"S"`
	N    *string         `json:"N"`
	B    []byte          `json:"B"`
	BOOL *bool           `json:"BOOL"`
	L    []attr          `json:"L"`
	M    map[string]attr `json:"M"`
}

// windowResponse always has a state object: Lambda counts a reply without one, or with
// null, as a failed invocation.
type windowResponse struct {
	State             struct{}      `json:"state"`
	BatchItemFailures []itemFailure `json:"batchItemFailures"`
}

type itemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

// Reasons that a failed invocation's log line gives.
const (
	reasonLifetime       = "lifetime"
	reasonInvalidRequest = "invalid_request"
	reasonFenced         = "fenced"
	reasonArchiveSchema  = "archive_schema"
	reasonArchive        = "archive"
	reasonEvent          = "event"
	reasonOther          = "other"
)

// projectorFn serves DYNAFLUX_MODE=projector. Its fields are set once, by newProjector:
// state kept from one invocation for the next would break pruning (S9 rule 1).
type projectorFn struct {
	core    *projector.Core
	streams map[string]string // stream table → stream
	guard   error             // the archive schema check (ADR 0008), nil when it passed
	log     *slog.Logger
	store   string // the metrics' Store dimension
}

// newProjector returns the projector of region e.region, which writes views to b and
// archives through s.
func newProjector(c *engine.Catalog, e env, b store.Backend, s firehose.Sender, log *slog.Logger) *projectorFn {
	cat := c.Catalog()
	naming := catalog.Naming{Prefix: e.prefix, Store: cat.Store}
	w := firehose.NewWriter(c.Projector(), naming, e.region, s)
	p := &projectorFn{
		core:    projector.New(c.Projector(), b, w, projector.Config{Naming: naming}),
		streams: map[string]string{},
		guard:   archive.CheckSchema(cat, e.region, e.archiveSchema),
		log:     log,
		store:   naming.Base(),
	}
	for _, st := range cat.Streams {
		p.streams[naming.StreamTable(st.Name)] = st.Name
	}
	return p
}

// Invoke implements aws-lambda-go's Handler. It applies the batch synchronously with
// Lambda's context, whose deadline the core stops sending before, so no request is in
// flight when it returns. An error fails the invocation, and Lambda retries the batch.
func (p *projectorFn) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	resp := windowResponse{BatchItemFailures: []itemFailure{}}
	var ev windowEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, p.fail(ctx, projector.Batch{}, reasonEvent, fmt.Errorf("runtime: decode the event: %w", err))
	}
	if len(ev.Records) == 0 {
		// The window's final invocation; until it succeeds, Lambda stops the shard.
		return json.Marshal(resp)
	}
	b := projector.Batch{Unit: ev.ShardID}
	if b.Unit == "" {
		return nil, p.fail(ctx, b, reasonEvent, errors.New("runtime: the event has records but no shardId: "+
			"the event source mapping needs TumblingWindowInSeconds (ADR 0003)"))
	}
	var ok bool
	if b.Stream, ok = p.streams[tableName(ev.EventSourceARN)]; !ok {
		return nil, p.fail(ctx, b, reasonEvent, fmt.Errorf("runtime: %s isn't the stream of a stream table of store %s",
			ev.EventSourceARN, p.store))
	}
	if p.guard != nil {
		return nil, p.fail(ctx, b, reasonArchiveSchema, p.guard)
	}
	var err error
	if b.Records, err = records(ev.Records); err != nil {
		return nil, p.fail(ctx, b, reasonEvent, err)
	}
	res, err := p.core.Handle(ctx, b)
	logCounts(ctx, p.log, p.store, b, res.Stats)
	if err != nil {
		return nil, p.fail(ctx, b, reason(err), err)
	}
	if res.FailedSeq != "" {
		attrs := append(batchAttrs(ctx, b), slog.String("failedSeq", res.FailedSeq), slog.String("error", cut(res.RowErr.Error(), errorBytes)))
		p.log.LogAttrs(ctx, slog.LevelWarn, "rows failed", attrs...)
		resp.BatchItemFailures = append(resp.BatchItemFailures, itemFailure{ItemIdentifier: res.FailedSeq})
	}
	return json.Marshal(resp)
}

// fail logs why the invocation fails, at level ERROR, and returns err. aws-lambda-go logs
// the returned error as plain text, which Lambda files as INFO.
func (p *projectorFn) fail(ctx context.Context, b projector.Batch, reason string, err error) error {
	attrs := append(batchAttrs(ctx, b), slog.String("reason", reason), slog.String("error", err.Error()))
	p.log.LogAttrs(ctx, slog.LevelError, "batch failed", attrs...)
	return err
}

// reason classifies an error of the core.
func reason(err error) string {
	switch {
	case errors.Is(err, projector.ErrLifetime):
		return reasonLifetime
	case errors.Is(err, meta.ErrFenced):
		return reasonFenced
	case errors.Is(err, projector.ErrArchive):
		return reasonArchive
	case errors.Is(err, store.ErrInvalidRequest):
		return reasonInvalidRequest
	}
	return reasonOther
}

// tableName returns the table of a stream ARN, arn:…:table/<table>/stream/<label>, or "".
// The ARN's region isn't read.
func tableName(arn string) string {
	_, rest, ok := strings.Cut(arn, ":table/")
	if !ok {
		return ""
	}
	table, _, ok := strings.Cut(rest, "/stream/")
	if !ok {
		return ""
	}
	return table
}

// records converts the event's records, REMOVEs included: record kinds are the core's
// business.
func records(in []streamRecord) ([]projector.Record, error) {
	out := make([]projector.Record, len(in))
	for i, r := range in {
		if r.Change.StreamViewType != streamView {
			return nil, fmt.Errorf("runtime: record %s has StreamViewType %q: the stream table's stream needs %s",
				r.Change.SequenceNumber, r.Change.StreamViewType, streamView)
		}
		var op projector.Op
		switch r.EventName {
		case "INSERT":
			op = projector.Insert
		case "MODIFY":
			op = projector.Modify
		case "REMOVE":
			op = projector.Remove
		default:
			return nil, fmt.Errorf("runtime: record %s has eventName %q", r.Change.SequenceNumber, r.EventName)
		}
		arrival, err := arrival(r.Change.ApproximateCreationDateTime)
		if err != nil {
			return nil, fmt.Errorf("runtime: record %s: ApproximateCreationDateTime: %w", r.Change.SequenceNumber, err)
		}
		out[i] = projector.Record{
			Op:      op,
			Seq:     r.Change.SequenceNumber,
			Arrival: arrival,
			New:     item(r.Change.NewImage),
			Old:     item(r.Change.OldImage),
		}
	}
	return out, nil
}

// arrival returns the epoch seconds n in whole seconds; the zero time without n.
func arrival(n json.Number) (time.Time, error) {
	if n == "" {
		return time.Time{}, nil
	}
	f, err := n.Float64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(f), 0).UTC(), nil
}

func item(m map[string]attr) store.Item {
	if m == nil {
		return nil
	}
	it := make(store.Item, len(m))
	for k, a := range m {
		it[k] = a.value()
	}
	return it
}

// value converts a. NULL and sets, which Dynaflux never writes, become the zero Value, which
// the core treats as a foreign value (encoding.md §1). Empty B, L and M stay non-nil, so
// their type survives.
func (a attr) value() store.Value {
	switch {
	case a.S != nil:
		return store.Value{Type: store.S, Str: *a.S}
	case a.N != nil:
		return store.Value{Type: store.N, Str: *a.N}
	case a.B != nil:
		return store.Value{Type: store.B, Bin: a.B}
	case a.BOOL != nil:
		return store.Value{Type: store.BOOL, Bool: *a.BOOL}
	case a.L != nil:
		l := make([]store.Value, len(a.L))
		for i, e := range a.L {
			l[i] = e.value()
		}
		return store.Value{Type: store.L, List: l}
	case a.M != nil:
		return store.Value{Type: store.M, Map: item(a.M)}
	}
	return store.Value{}
}
