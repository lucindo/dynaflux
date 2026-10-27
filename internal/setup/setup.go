// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package setup answers the CloudFormation custom resources of regional.yaml, which the
// runtime binary serves in its setup mode (ADR 0008, docs/spec/infra.md §6.2, §7.2):
//   - [TypeStreamARN] returns the stream ARN of a stream table's replica in the function's
//     region, which an event source mapping needs and CloudFormation can't give.
//   - [TypeArchiveSchema] creates the archive's Glue database, creates its Iceberg tables
//     and adds their new columns with Athena DDL, creates or updates their Glue table
//     optimizers, and re-creates their dedup views.
//   - [TypeCatalog] registers the catalog that the binary runs in the meta table, and sets
//     retired the versions it lists retired (ADR 0013).
//
// The handlers work from the request's properties alone, except that the catalog handler
// also reads the binary's catalog. They reach AWS through small interfaces that the runtime
// implements with the AWS SDK, and the meta table through its storage backend.
package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lucindo/dynaflux/internal/store"
)

// The custom resource types.
const (
	TypeStreamARN     = "Custom::DynafluxStreamArn"
	TypeArchiveSchema = "Custom::DynafluxArchiveSchema"
	TypeCatalog       = "Custom::DynafluxCatalog"
)

// The request types.
const (
	ActionCreate = "Create"
	ActionUpdate = "Update"
	ActionDelete = "Delete"
)

// Errors that the interfaces' implementations wrap, so the handler can retry them.
var (
	// ErrAccessDenied is AWS refusing a call for lack of permission, including Glue refusing
	// a role that it can't assume yet.
	ErrAccessDenied = errors.New("access denied")
	// ErrConflict is a write that failed because another writer changed the same thing
	// first, such as Firehose committing to the Iceberg table during an Athena statement.
	ErrConflict = errors.New("conflict with another writer")
)

// New roles and grants, which a deploy can create or change just before a request, and other
// writers' Iceberg commits settle within seconds; a minute of retries rides them out.
const (
	retryFor   = time.Minute
	retryEvery = 5 * time.Second
)

// DynamoDB describes tables in the function's region.
type DynamoDB interface {
	// DescribeTable returns the table named name, or false when it doesn't exist.
	DescribeTable(ctx context.Context, name string) (StreamTable, bool, error)
}

// Athena runs statements in a workgroup.
type Athena interface {
	// Exec runs sql in workgroup and waits for it to finish. It stops the query when ctx
	// is done, and fails when the query fails.
	Exec(ctx context.Context, workgroup, sql string) error
}

// Glue reads and writes the Glue Data Catalog.
type Glue interface {
	// CreateDatabase creates database name at location, and does nothing when it exists.
	CreateDatabase(ctx context.Context, name, location string) error
	// Table returns table in database db, or false when it doesn't exist.
	Table(ctx context.Context, db, table string) (GlueTable, bool, error)
	// PutOptimizer creates the table's optimizer of o's type, enabled, or updates it.
	PutOptimizer(ctx context.Context, db, table string, o Optimizer) error
}

// StreamTable is a stream table as DescribeTable reports it in the function's region.
type StreamTable struct {
	// Status is the TableStatus, such as "ACTIVE".
	Status string
	// StreamARN is the LatestStreamArn, empty when the table has no stream.
	StreamARN string
	// StreamView is the stream's StreamViewType, such as "NEW_AND_OLD_IMAGES".
	StreamView string
	// ReplicaStatus is the ReplicaStatus of the Replicas entry for the function's region,
	// empty when DescribeTable lists none for it.
	ReplicaStatus string
}

// GlueTable is a table of the Glue Data Catalog.
type GlueTable struct {
	Location string
	// TableType is the table_type parameter, "ICEBERG" for an Iceberg table.
	TableType string
	// Columns maps each column's name to its type.
	Columns map[string]string
}

// Glue's table optimizer types.
const (
	OptimizerCompaction = "compaction"
	OptimizerRetention  = "retention"
)

// Optimizer is a Glue table optimizer, which Glue runs as Role.
type Optimizer struct {
	// Type is OptimizerCompaction or OptimizerRetention.
	Type string
	Role string
	// Strategy is a compaction optimizer's strategy, such as "binpack".
	Strategy string
	// A retention optimizer expires the snapshots older than RetentionDays, keeping at least
	// SnapshotsToKeep, and deletes their files.
	RetentionDays   int
	SnapshotsToKeep int
}

// Request is a CloudFormation custom resource request.
type Request struct {
	// Type is the resource type: TypeStreamARN, TypeArchiveSchema or TypeCatalog.
	Type string
	// Action is the request type: ActionCreate, ActionUpdate or ActionDelete.
	Action string
	// PhysicalID is the resource's physical ID, empty on ActionCreate.
	PhysicalID string
	// Props are the resource's properties. CloudFormation passes every scalar as a string.
	Props map[string]any
}

// Response is the reply to a request: the resource's physical ID, and the attributes that
// Fn::GetAtt reads.
type Response struct {
	PhysicalID string
	Data       map[string]string
}

// Handler answers custom resource requests.
type Handler struct {
	DynamoDB DynamoDB
	Athena   Athena
	Glue     Glue
	// Meta is the backend of the meta table, and Catalog the bytes of the catalog that the
	// binary runs, as catalog.Marshal returns them.
	Meta    store.Backend
	Catalog []byte
}

// Handle answers r. Delete does nothing but for the catalog: the archive schema is
// retained, and a stream lookup owns nothing. On an error, the response keeps r's physical
// ID, so a failed update isn't taken for a replacement.
func (h *Handler) Handle(ctx context.Context, r Request) (Response, error) {
	var resp Response
	var err error
	switch {
	case r.Type == TypeCatalog:
		resp, err = h.registerCatalog(ctx, r)
	case r.Action == ActionDelete:
		// Whatever the type: a create that failed is rolled back with a delete.
		return Response{PhysicalID: r.PhysicalID}, nil
	case r.Type == TypeStreamARN:
		resp, err = h.lookupStream(ctx, r.Props)
	case r.Type == TypeArchiveSchema:
		resp, err = h.applySchema(ctx, r.Props)
	default:
		err = fmt.Errorf("unknown resource type %q", r.Type)
	}
	if err != nil {
		return Response{PhysicalID: r.PhysicalID}, err
	}
	return resp, nil
}

// decode reads props into v, whose fields' JSON tags name the properties.
func decode(props map[string]any, v any) error {
	data, err := json.Marshal(props)
	if err != nil {
		return fmt.Errorf("read the properties: %w", err)
	}
	if err = json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("read the properties: %w", err)
	}
	return nil
}

// retry calls f until it returns an error other than ErrAccessDenied and ErrConflict, for up
// to retryFor.
func retry(ctx context.Context, f func() error) error {
	deadline := time.Now().Add(retryFor)
	for {
		err := f()
		if !errors.Is(err, ErrAccessDenied) && !errors.Is(err, ErrConflict) || !time.Now().Before(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(retryEvery):
		}
	}
}
