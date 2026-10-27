// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package awssetup implements the interfaces of internal/setup with the AWS SDK, for the
// setup function of the runtime binary (ADR 0008). It maps AWS's errors onto setup's: a
// refusal for lack of permission becomes [setup.ErrAccessDenied], and a write that met
// another writer's becomes [setup.ErrConflict], both of which the handlers retry.
package awssetup

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/setup"
)

// dynamoAPI, glueAPI and athenaAPI are every operation the adapters send: the upper bound
// of what the setup role must grant.
type (
	dynamoAPI interface {
		DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	}
	glueAPI interface {
		CreateDatabase(ctx context.Context, in *glue.CreateDatabaseInput, opts ...func(*glue.Options)) (*glue.CreateDatabaseOutput, error)
		GetTable(ctx context.Context, in *glue.GetTableInput, opts ...func(*glue.Options)) (*glue.GetTableOutput, error)
		CreateTableOptimizer(ctx context.Context, in *glue.CreateTableOptimizerInput, opts ...func(*glue.Options)) (*glue.CreateTableOptimizerOutput, error)
		UpdateTableOptimizer(ctx context.Context, in *glue.UpdateTableOptimizerInput, opts ...func(*glue.Options)) (*glue.UpdateTableOptimizerOutput, error)
	}
	athenaAPI interface {
		StartQueryExecution(ctx context.Context, in *athena.StartQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
		GetQueryExecution(ctx context.Context, in *athena.GetQueryExecutionInput, opts ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
		StopQueryExecution(ctx context.Context, in *athena.StopQueryExecutionInput, opts ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
	}
)

// New returns a handler whose calls go through clients of cfg, whose Region is the
// function's region. It sets no client option: retries, timeouts and endpoints are cfg's.
func New(cfg aws.Config) *setup.Handler {
	return &setup.Handler{
		DynamoDB: &dynamoDB{c: dynamodb.NewFromConfig(cfg), region: cfg.Region},
		Glue:     &glueCatalog{c: glue.NewFromConfig(cfg)},
		Athena:   &athenaRunner{c: athena.NewFromConfig(cfg)},
	}
}

// classify wraps AWS's refusals for lack of permission in setup.ErrAccessDenied. Every
// service here names it AccessDeniedException.
func classify(err error) error {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok && apiErr.ErrorCode() == "AccessDeniedException" {
		return fmt.Errorf("%w: %w", setup.ErrAccessDenied, err)
	}
	return err
}
