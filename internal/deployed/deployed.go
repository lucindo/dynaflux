// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package deployed reads one region of a deployed store for the operator commands: its
// registered catalog, and from the services around the meta table the backfill jobs'
// executions, the event source mappings, the archive's metrics, and the gaps that Lambda
// recorded in the store's bucket. Each service is a small interface that
// the AWS SDK's client satisfies as it is.
package deployed

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/lucindo/dynaflux/internal/meta"
)

// The services, by the operations the commands send.
type (
	// Lambda lists event source mappings.
	Lambda interface {
		ListEventSourceMappings(ctx context.Context, in *lambda.ListEventSourceMappingsInput, opts ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error)
	}
	// CloudWatch reads metrics.
	CloudWatch interface {
		GetMetricData(ctx context.Context, in *cloudwatch.GetMetricDataInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
	}
	// S3 lists and reads the store's bucket.
	S3 interface {
		ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
		GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	}
	// StepFunctions describes the backfill state machine's executions.
	StepFunctions interface {
		DescribeExecution(ctx context.Context, in *sfn.DescribeExecutionInput, opts ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error)
	}
)

// ErrNotDeployed is a region whose meta table holds no registered catalog, or doesn't exist,
// as with a prefix that isn't deployed.
var ErrNotDeployed = errors.New("deployed: no catalog is registered")

// Catalog is a region's registered catalog. Its format may be newer than this build reads.
type Catalog struct {
	meta.Registered

	Data []byte // the bytes that catalog.Marshal returned
}

// ReadCatalog reads the catalog registered in the meta table of m.
func ReadCatalog(ctx context.Context, m *meta.Meta) (Catalog, error) {
	reg, ok, err := m.Current(ctx)
	if _, missing := errors.AsType[*types.ResourceNotFoundException](err); missing || err == nil && !ok {
		return Catalog{}, ErrNotDeployed
	}
	if err != nil {
		return Catalog{}, err
	}
	data, err := m.ReadCatalog(ctx, reg.Hash)
	if err != nil {
		return Catalog{}, err
	}
	return Catalog{Registered: reg, Data: data}, nil
}
