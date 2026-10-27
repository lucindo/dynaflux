// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package awsbackfill implements the interfaces of internal/backfill with the AWS SDK, for
// the backfill function of the runtime binary and for dynaflux backfill: staged runs in the
// region's bucket, a stream's shards, the replicas of a stream table, and the projectors'
// lag. The archive read is M6's.
package awsbackfill

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3API, DynamoDB, streamsAPI and CloudWatch are every operation the adapters send: the upper
// bound of what the backfill role must grant, besides the store's calls.
type (
	s3API interface {
		PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
		GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
		ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
		DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	}
	// DynamoDB describes tables.
	DynamoDB interface {
		DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	}
	streamsAPI interface {
		DescribeStream(ctx context.Context, in *dynamodbstreams.DescribeStreamInput, opts ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error)
	}
	// CloudWatch reads metrics.
	CloudWatch interface {
		GetMetricData(ctx context.Context, in *cloudwatch.GetMetricDataInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
	}
)
