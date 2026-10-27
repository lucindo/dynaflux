// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/setup"
)

// dynamoDB implements [setup.DynamoDB] in region.
type dynamoDB struct {
	c      dynamoAPI
	region string
}

// DescribeTable implements [setup.DynamoDB]. A table whose stream is disabled has no stream
// ARN, though DynamoDB still reports the last one.
func (d *dynamoDB) DescribeTable(ctx context.Context, name string) (setup.StreamTable, bool, error) {
	out, err := d.c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name})
	if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
		return setup.StreamTable{}, false, nil
	}
	if err != nil {
		return setup.StreamTable{}, false, classify(err)
	}
	t := out.Table
	st := setup.StreamTable{Status: string(t.TableStatus)}
	if s := t.StreamSpecification; s != nil && aws.ToBool(s.StreamEnabled) {
		st.StreamARN, st.StreamView = aws.ToString(t.LatestStreamArn), string(s.StreamViewType)
	}
	for _, r := range t.Replicas {
		if aws.ToString(r.RegionName) == d.region {
			st.ReplicaStatus = string(r.ReplicaStatus)
		}
	}
	return st, true, nil
}
