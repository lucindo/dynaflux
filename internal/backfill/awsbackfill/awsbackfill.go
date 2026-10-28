// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package awsbackfill implements the interfaces of internal/backfill with the AWS SDK, for
// the backfill function of the runtime binary and for dynaflux backfill: staged runs in the
// region's bucket, a stream's shards, the replicas of a stream table, the projectors' lag,
// and the archive with what AWS shows of its streams' history.
package awsbackfill

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The services, by every operation the adapters send: the upper bound of what the backfill
// role must grant, besides the store's calls. The SDK's clients satisfy them.
type (
	// S3 reads and writes the region's bucket.
	S3 interface {
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
	// MetricLister lists metrics.
	MetricLister interface {
		ListMetrics(ctx context.Context, in *cloudwatch.ListMetricsInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error)
	}
	// Athena runs queries on the archive.
	Athena interface {
		StartQueryExecution(ctx context.Context, in *athena.StartQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
		GetQueryExecution(ctx context.Context, in *athena.GetQueryExecutionInput, opts ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
		GetQueryResults(ctx context.Context, in *athena.GetQueryResultsInput, opts ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
		StopQueryExecution(ctx context.Context, in *athena.StopQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
	}
	// Firehose describes delivery streams.
	Firehose interface {
		DescribeDeliveryStream(ctx context.Context, in *firehose.DescribeDeliveryStreamInput, opts ...func(*firehose.Options)) (*firehose.DescribeDeliveryStreamOutput, error)
	}
	// Lambda lists event source mappings.
	Lambda interface {
		ListEventSourceMappings(ctx context.Context, in *lambda.ListEventSourceMappingsInput, opts ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error)
	}
)

// Clients are the clients of a region that [Archive] calls.
type Clients struct {
	DynamoDB   DynamoDB
	S3         S3
	CloudWatch CloudWatch
	Metrics    MetricLister
	Athena     Athena
	Firehose   Firehose
	Lambda     Lambda
}

// NewClients returns the SDK's clients of cfg.
func NewClients(cfg aws.Config) Clients {
	metrics := cloudwatch.NewFromConfig(cfg)
	return Clients{
		DynamoDB: dynamodb.NewFromConfig(cfg),
		// A custom endpoint, a test's, serves no bucket names as hosts.
		S3:         s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = o.BaseEndpoint != nil }),
		CloudWatch: metrics,
		Metrics:    metrics,
		Athena:     athena.NewFromConfig(cfg),
		Firehose:   firehose.NewFromConfig(cfg),
		Lambda:     lambda.NewFromConfig(cfg),
	}
}
