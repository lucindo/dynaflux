// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/setup"
)

// glueCatalog implements [setup.Glue] on the Glue Data Catalog.
type glueCatalog struct {
	c glueAPI
}

// CreateDatabase implements [setup.Glue].
func (g *glueCatalog) CreateDatabase(ctx context.Context, name, location string) error {
	_, err := g.c.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: &name, LocationUri: &location},
	})
	if _, ok := errors.AsType[*types.AlreadyExistsException](err); ok {
		return nil
	}
	return classify(err)
}

// Table implements [setup.Glue]. Glue answers EntityNotFound also when the database is
// missing.
func (g *glueCatalog) Table(ctx context.Context, db, table string) (setup.GlueTable, bool, error) {
	out, err := g.c.GetTable(ctx, &glue.GetTableInput{DatabaseName: &db, Name: &table})
	if _, ok := errors.AsType[*types.EntityNotFoundException](err); ok {
		return setup.GlueTable{}, false, nil
	}
	if err != nil {
		return setup.GlueTable{}, false, classify(err)
	}
	t := setup.GlueTable{TableType: out.Table.Parameters["table_type"], Columns: map[string]string{}}
	if sd := out.Table.StorageDescriptor; sd != nil {
		t.Location = aws.ToString(sd.Location)
		for _, c := range sd.Columns {
			// Iceberg's Glue catalog keeps the columns of earlier schemas, marked not current.
			if c.Parameters["iceberg.field.current"] != "false" {
				t.Columns[aws.ToString(c.Name)] = aws.ToString(c.Type)
			}
		}
	}
	return t, true, nil
}

// PutOptimizer implements [setup.Glue]: it creates the optimizer, and updates it when it
// exists. The optimizer's catalog is the account of its role.
func (g *glueCatalog) PutOptimizer(ctx context.Context, db, table string, o setup.Optimizer) error {
	conf, err := optimizerConfig(o)
	if err != nil {
		return err
	}
	a, err := arn.Parse(o.Role)
	if err != nil || a.Service != "iam" || a.AccountID == "" || !strings.HasPrefix(a.Resource, "role/") {
		return fmt.Errorf("optimizer role %q isn't an IAM role ARN", o.Role)
	}
	account, typ := a.AccountID, types.TableOptimizerType(o.Type)
	_, err = g.c.CreateTableOptimizer(ctx, &glue.CreateTableOptimizerInput{
		CatalogId: &account, DatabaseName: &db, TableName: &table, Type: typ, TableOptimizerConfiguration: conf,
	})
	if _, ok := errors.AsType[*types.AlreadyExistsException](err); ok {
		_, err = g.c.UpdateTableOptimizer(ctx, &glue.UpdateTableOptimizerInput{
			CatalogId: &account, DatabaseName: &db, TableName: &table, Type: typ, TableOptimizerConfiguration: conf,
		})
	}
	return classifyOptimizer(err, o.Role)
}

// optimizerConfig returns o as Glue configures it, enabled.
func optimizerConfig(o setup.Optimizer) (*types.TableOptimizerConfiguration, error) {
	conf := &types.TableOptimizerConfiguration{RoleArn: &o.Role, Enabled: new(true)}
	switch o.Type {
	case setup.OptimizerCompaction:
		conf.CompactionConfiguration = &types.CompactionConfiguration{
			IcebergConfiguration: &types.IcebergCompactionConfiguration{Strategy: types.CompactionStrategy(o.Strategy)},
		}
	case setup.OptimizerRetention:
		if o.RetentionDays < 0 || o.RetentionDays > math.MaxInt32 || o.SnapshotsToKeep < 0 || o.SnapshotsToKeep > math.MaxInt32 {
			return nil, fmt.Errorf("retention of %d days keeping %d snapshots is out of range", o.RetentionDays, o.SnapshotsToKeep)
		}
		conf.RetentionConfiguration = &types.RetentionConfiguration{IcebergConfiguration: &types.IcebergRetentionConfiguration{
			SnapshotRetentionPeriodInDays: new(int32(o.RetentionDays)),
			NumberOfSnapshotsToRetain:     new(int32(o.SnapshotsToKeep)),
			CleanExpiredFiles:             new(true),
		}}
	default:
		return nil, fmt.Errorf("unknown optimizer type %q", o.Type)
	}
	return conf, nil
}

// classifyOptimizer classifies an error of an optimizer call. Glue refuses a role it can't
// assume yet, such as one the same deploy just created, with an invalid-input error that
// names the role; that clears as IAM propagates, as a missing grant does.
func classifyOptimizer(err error, role string) error {
	if _, ok := errors.AsType[*types.ConcurrentModificationException](err); ok {
		return fmt.Errorf("%w: %w", setup.ErrConflict, err)
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		switch msg := apiErr.ErrorMessage(); apiErr.ErrorCode() {
		case "InvalidInputException", "ValidationException":
			if strings.Contains(msg, role) || strings.Contains(strings.ToLower(msg), "assume") {
				return fmt.Errorf("%w: %w", setup.ErrAccessDenied, err)
			}
		}
	}
	return classify(err)
}
