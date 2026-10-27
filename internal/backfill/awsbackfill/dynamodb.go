// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
)

// Units lists the shards of a stream table's stream in the region: [backfill.Lister].
type Units struct {
	ddb     DynamoDB
	streams streamsAPI
}

// NewUnits returns the lister whose calls go to ddb and streams.
func NewUnits(ddb DynamoDB, streams streamsAPI) *Units {
	return &Units{ddb: ddb, streams: streams}
}

// Units implements [backfill.Lister]: every shard of the table's stream, open or closed.
func (u *Units) Units(ctx context.Context, streamTable string) ([]string, error) {
	arn, err := StreamARN(ctx, u.ddb, streamTable)
	if err != nil {
		return nil, err
	}
	if arn == "" {
		return nil, fmt.Errorf("awsbackfill: %s has no stream", streamTable)
	}
	var shards []string
	in := &dynamodbstreams.DescribeStreamInput{StreamArn: &arn}
	for {
		page, derr := u.streams.DescribeStream(ctx, in)
		if derr != nil {
			return nil, fmt.Errorf("awsbackfill: describe the stream of %s: %w", streamTable, derr)
		}
		for _, s := range page.StreamDescription.Shards {
			shards = append(shards, aws.ToString(s.ShardId))
		}
		if page.StreamDescription.LastEvaluatedShardId == nil {
			return shards, nil
		}
		in.ExclusiveStartShardId = page.StreamDescription.LastEvaluatedShardId
	}
}

// StreamARN returns the ARN of the latest change stream of table, or "" when its stream is
// off.
func StreamARN(ctx context.Context, c DynamoDB, table string) (string, error) {
	out, err := c.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return "", fmt.Errorf("awsbackfill: describe %s: %w", table, err)
	}
	if s := out.Table.StreamSpecification; s == nil || !aws.ToBool(s.StreamEnabled) {
		return "", nil
	}
	return aws.ToString(out.Table.LatestStreamArn), nil
}

// Replicas describes the replicas of a stream table: [backfill.Tables].
type Replicas struct {
	region string
	client func(region string) DynamoDB
}

// NewReplicas returns the replicas as cfg's region sees them; it describes each other replica
// with a client of cfg in that replica's region.
func NewReplicas(cfg aws.Config) *Replicas {
	return &Replicas{region: cfg.Region, client: func(region string) DynamoDB {
		return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.Region = region })
	}}
}

// Born implements [backfill.Tables]: it describes the table in the region, then in the
// region of each replica it lists.
func (r *Replicas) Born(ctx context.Context, streamTable string) (time.Time, bool, error) {
	local, err := r.describe(ctx, r.region, streamTable)
	if err != nil {
		return time.Time{}, false, err
	}
	born, restored := aws.ToTime(local.CreationDateTime), local.RestoreSummary != nil
	for _, rep := range local.Replicas {
		region := aws.ToString(rep.RegionName)
		if region == r.region {
			continue
		}
		t, derr := r.describe(ctx, region, streamTable)
		if derr != nil {
			return time.Time{}, false, derr
		}
		if c := aws.ToTime(t.CreationDateTime); c.Before(born) {
			born = c
		}
		restored = restored || t.RestoreSummary != nil
	}
	return born, restored, nil
}

func (r *Replicas) describe(ctx context.Context, region, table string) (*types.TableDescription, error) {
	out, err := r.client(region).DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, fmt.Errorf("awsbackfill: describe %s in %s: %w", table, region, err)
	}
	return out.Table, nil
}
