// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package localstore creates and deletes a store's tables on DynamoDB Local as the manifest of
// internal/infra declares them: keys, on-demand billing, the stream view and the TTL
// attribute, so a disagreement between the templates and the runtime shows locally. dynaflux
// dev and the emulator tests use it.
package localstore

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/infra"
)

// Table is a table of a store, by its name with the prefix.
type Table struct {
	Name string
	Spec infra.Table
}

// Tables returns the tables of m's global and regional scopes, named with prefix.
func Tables(m *infra.Manifest, prefix string) ([]Table, error) {
	var out []Table
	for _, spec := range slices.Concat(m.Global.Tables, m.Regional.Tables) {
		name, err := m.Resolve(spec.Name, infra.Deployment{Prefix: prefix})
		if err != nil {
			return nil, fmt.Errorf("localstore: table %s: %w", spec.ID, err)
		}
		out = append(out, Table{Name: name, Spec: spec})
	}
	return out, nil
}

// Ensure creates each table of ts that doesn't exist, and returns the names of those it
// created. An existing table is left as it is, whatever its spec.
func Ensure(ctx context.Context, c *dynamodb.Client, ts []Table) ([]string, error) {
	var created []string
	for _, t := range ts {
		ok, err := create(ctx, c, t)
		if err != nil {
			return created, fmt.Errorf("localstore: create table %s: %w", t.Name, err)
		}
		if ok {
			created = append(created, t.Name)
		}
	}
	return created, nil
}

// Delete deletes each table of ts that exists, and returns the names of those it deleted.
func Delete(ctx context.Context, c *dynamodb.Client, ts []Table) ([]string, error) {
	var deleted []string
	for _, t := range ts {
		_, err := c.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: &t.Name})
		if _, missing := errors.AsType[*types.ResourceNotFoundException](err); missing {
			continue
		}
		if err != nil {
			return deleted, fmt.Errorf("localstore: delete table %s: %w", t.Name, err)
		}
		deleted = append(deleted, t.Name)
	}
	return deleted, nil
}

// create creates t, and reports false when it exists.
func create(ctx context.Context, c *dynamodb.Client, t Table) (bool, error) {
	in := &dynamodb.CreateTableInput{TableName: &t.Name, BillingMode: types.BillingModePayPerRequest}
	for _, k := range t.Spec.Keys {
		var kt types.KeyType
		switch k.Key {
		case infra.KeyHash:
			kt = types.KeyTypeHash
		case infra.KeyRange:
			kt = types.KeyTypeRange
		default:
			return false, fmt.Errorf("key %s has role %q", k.Name, k.Key)
		}
		in.AttributeDefinitions = append(in.AttributeDefinitions, types.AttributeDefinition{
			AttributeName: new(k.Name), AttributeType: types.ScalarAttributeType(k.Type),
		})
		in.KeySchema = append(in.KeySchema, types.KeySchemaElement{AttributeName: new(k.Name), KeyType: kt})
	}
	switch t.Spec.StreamView {
	case "":
	case infra.StreamViewNewAndOld:
		in.StreamSpecification = &types.StreamSpecification{StreamEnabled: new(true), StreamViewType: types.StreamViewTypeNewAndOldImages}
	default:
		return false, fmt.Errorf("stream view %q", t.Spec.StreamView)
	}
	_, err := c.CreateTable(ctx, in)
	if _, exists := errors.AsType[*types.ResourceInUseException](err); exists {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if t.Spec.TTLAttribute != "" {
		_, err = c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName:               &t.Name,
			TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: &t.Spec.TTLAttribute, Enabled: new(true)},
		})
		if err != nil {
			return false, fmt.Errorf("TTL: %w", err)
		}
	}
	return true, nil
}
