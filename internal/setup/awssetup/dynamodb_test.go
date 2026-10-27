// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup_test

import (
	"errors"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	streamTable = "prod-analytics-stream-page_views"
	streamARN   = "arn:aws:dynamodb:us-east-1:123456789012:table/prod-analytics-stream-page_views/stream/2026-10-26T12:00:00.000"
)

// describeBody is DescribeTable's answer for the stream table of a Global Table whose
// replica in us-east-1, the adapters' region, is still being created.
const describeBody = `{"Table":{
  "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
  "BillingModeSummary":{"BillingMode":"PAY_PER_REQUEST","LastUpdateToPayPerRequestDateTime":1.761480000E9},
  "CreationDateTime":1.761480000E9,
  "DeletionProtectionEnabled":true,
  "GlobalTableVersion":"2019.11.21",
  "ItemCount":0,
  "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],
  "LatestStreamArn":"` + streamARN + `",
  "LatestStreamLabel":"2026-10-26T12:00:00.000",
  "ProvisionedThroughput":{"NumberOfDecreasesToday":0,"ReadCapacityUnits":0,"WriteCapacityUnits":0},
  "Replicas":[
    {"RegionName":"us-west-2","ReplicaStatus":"ACTIVE"},
    {"RegionName":"us-east-1","ReplicaStatus":"CREATING"},
    {"RegionName":"eu-west-1","ReplicaStatus":"ACTIVE"}
  ],
  "StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"},
  "TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/prod-analytics-stream-page_views",
  "TableId":"8f4e2c1a-3b5d-4e6f-9a7b-0c1d2e3f4a5b",
  "TableName":"prod-analytics-stream-page_views",
  "TableSizeBytes":0,
  "TableStatus":"UPDATING",
  "TimeToLiveDescription":{"AttributeName":"_ttl","TimeToLiveStatus":"ENABLED"}
}}`

// streamDisabledBody is a table whose stream was turned off: DynamoDB still reports the
// last stream's ARN.
const streamDisabledBody = `{"Table":{
  "LatestStreamArn":"` + streamARN + `",
  "LatestStreamLabel":"2026-10-26T12:00:00.000",
  "StreamSpecification":{"StreamEnabled":false},
  "TableName":"prod-analytics-stream-page_views",
  "TableStatus":"ACTIVE"
}}`

func TestDescribeTable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		answer answer
		want   setup.StreamTable
		found  bool
		denied bool // the error is ErrAccessDenied; otherwise none unless failed
		failed bool
	}{
		{
			"replica of this region", ok(describeBody),
			setup.StreamTable{Status: "UPDATING", StreamARN: streamARN, StreamView: "NEW_AND_OLD_IMAGES", ReplicaStatus: "CREATING"},
			true, false, false,
		},
		{"stream disabled", ok(streamDisabledBody), setup.StreamTable{Status: "ACTIVE"}, true, false, false},
		{
			"missing", awsError(http.StatusBadRequest, "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
				"Requested resource not found: Table: prod-analytics-stream-page_views not found"),
			setup.StreamTable{}, false, false, false,
		},
		{
			"access denied", awsError(http.StatusBadRequest, "com.amazon.coral.service#AccessDeniedException",
				"User: arn:aws:sts::123456789012:assumed-role/prod-analytics-setup/prod-analytics-setup is not authorized to "+
					"perform: dynamodb:DescribeTable on resource: arn:aws:dynamodb:us-east-1:123456789012:table/"+streamTable+
					" because no identity-based policy allows the dynamodb:DescribeTable action"),
			setup.StreamTable{}, false, true, true,
		},
		{
			"server error", awsError(http.StatusInternalServerError, "com.amazonaws.dynamodb.v20120810#InternalServerError",
				"Internal server error"),
			setup.StreamTable{}, false, false, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := newFakeAWS(t, map[string][]answer{"DescribeTable": {tt.answer}})
				got, found, err := handler(f).DynamoDB.DescribeTable(t.Context(), streamTable)
				if got != tt.want || found != tt.found || (err != nil) != tt.failed || errors.Is(err, setup.ErrAccessDenied) != tt.denied {
					t.Errorf("got %+v, %v, %v\nwant %+v, %v, failed %v, access denied %v", got, found, err, tt.want, tt.found, tt.failed, tt.denied)
				}
				if calls := f.recorded(); len(calls) == 0 || calls[0].op != "DescribeTable" || calls[0].body != `{"TableName":"`+streamTable+`"}` {
					t.Errorf("calls %+v, want DescribeTable of %s", calls, streamTable)
				}
			})
		})
	}
}
