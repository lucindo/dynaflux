// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/lucindo/dynaflux/internal/esmlocal"
	"github.com/lucindo/dynaflux/internal/projector"
	"github.com/lucindo/dynaflux/internal/streamevent"
)

// function is the projector function of stream, without an archive, as Lambda would call it.
// It prints a line for each batch.
func (d *devRun) function(stream string) esmlocal.Function {
	return func(ctx context.Context, event []byte) ([]byte, error) {
		var ev streamevent.Event
		if err := json.Unmarshal(event, &ev); err != nil {
			return nil, fmt.Errorf("decode the event: %w", err)
		}
		recs, err := streamevent.Records(ev.Records)
		if err != nil {
			return nil, err
		}
		res, err := d.core.Handle(ctx, projector.Batch{Stream: stream, Unit: ev.ShardID, Records: recs})
		line := fmt.Sprintf("%s: %d records", stream, len(recs))
		if len(recs) == 1 {
			line = stream + ": 1 record"
		}
		switch {
		case err != nil:
			line += "; failed: " + err.Error()
		case res.FailedSeq != "":
			line += fmt.Sprintf("; rows failed from record %s: %v", res.FailedSeq, res.RowErr)
		}
		if len(res.Stats.Broken) > 0 {
			line += "; broke " + strings.Join(res.Stats.Broken, ", ")
		}
		d.c.errorf("%s", line)
		if err != nil {
			return nil, err
		}
		reply := streamevent.Reply{BatchItemFailures: []streamevent.ItemFailure{}}
		if res.FailedSeq != "" {
			reply.BatchItemFailures = append(reply.BatchItemFailures, streamevent.ItemFailure{ItemIdentifier: res.FailedSeq})
		}
		return json.Marshal(reply)
	}
}
