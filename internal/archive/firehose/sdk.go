// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package firehose

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

// noRecordID is the code of a record that Firehose answered without a RecordId or an
// ErrorCode, which counts as not accepted.
const noRecordID = "NoRecordId"

// SDK is the production [Sender].
type SDK struct {
	Client *fh.Client
}

// PutBatch implements [Sender].
func (s SDK) PutBatch(ctx context.Context, stream string, recs [][]byte) ([]string, error) {
	in := &fh.PutRecordBatchInput{DeliveryStreamName: &stream, Records: make([]types.Record, len(recs))}
	for i, r := range recs {
		in.Records[i] = types.Record{Data: r}
	}
	out, err := s.Client.PutRecordBatch(ctx, in)
	if err != nil {
		return nil, err
	}
	codes := make([]string, len(out.RequestResponses))
	for i, r := range out.RequestResponses {
		switch {
		case aws.ToString(r.ErrorCode) != "":
			codes[i] = *r.ErrorCode
		case aws.ToString(r.RecordId) == "":
			codes[i] = noRecordID
		}
	}
	return codes, nil
}
