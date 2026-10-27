// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package streamevent decodes the events of a DynamoDB stream with a tumbling window (ADR
// 0003), as Lambda delivers them to a projector function and esmlocal does in dynaflux dev,
// and holds the function's reply.
package streamevent

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/store"
)

// Event is the part of an event that the projector reads. Fields it doesn't declare can't
// fail decoding, which would block the shard for good.
type Event struct {
	Records        []Record `json:"Records"`
	ShardID        string   `json:"shardId"`
	EventSourceARN string   `json:"eventSourceARN"`
}

// Record is a stream record of an event.
type Record struct {
	EventName string `json:"eventName"`
	Change    Change `json:"dynamodb"`
}

// Change is a stream record's change.
type Change struct {
	ApproximateCreationDateTime json.Number                `json:"ApproximateCreationDateTime"`
	SequenceNumber              string                     `json:"SequenceNumber"`
	StreamViewType              string                     `json:"StreamViewType"`
	NewImage                    map[string]store.JSONValue `json:"NewImage"`
	OldImage                    map[string]store.JSONValue `json:"OldImage"`
}

// StreamView is the only view type whose records carry the images the projector reads.
const StreamView = "NEW_AND_OLD_IMAGES"

// Reply always has a state object: Lambda counts a reply without one, or with null, as a
// failed invocation.
type Reply struct {
	State             struct{}      `json:"state"`
	BatchItemFailures []ItemFailure `json:"batchItemFailures"`
}

// ItemFailure names the record from which Lambda retries the batch.
type ItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

// Records converts an event's records, REMOVEs included: record kinds are the projector's
// business. An error names the record.
func Records(in []Record) ([]projector.Record, error) {
	out := make([]projector.Record, len(in))
	for i, r := range in {
		seq := r.Change.SequenceNumber
		if r.Change.StreamViewType != StreamView {
			return nil, fmt.Errorf("record %s has StreamViewType %q: the stream table's stream needs %s", seq, r.Change.StreamViewType, StreamView)
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
			return nil, fmt.Errorf("record %s has eventName %q", seq, r.EventName)
		}
		arrival, err := arrival(r.Change.ApproximateCreationDateTime)
		if err != nil {
			return nil, fmt.Errorf("record %s: ApproximateCreationDateTime: %w", seq, err)
		}
		rec := projector.Record{Op: op, Seq: seq, Arrival: arrival}
		if rec.New, err = image(r.Change.NewImage); err != nil {
			return nil, fmt.Errorf("record %s: NewImage: %w", seq, err)
		}
		if rec.Old, err = image(r.Change.OldImage); err != nil {
			return nil, fmt.Errorf("record %s: OldImage: %w", seq, err)
		}
		out[i] = rec
	}
	return out, nil
}

// image decodes a record's image; nil when the record has none.
func image(m map[string]store.JSONValue) (store.Item, error) {
	if m == nil {
		return nil, nil //nolint:nilnil // a record without the image
	}
	return store.DecodeJSON(m)
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
