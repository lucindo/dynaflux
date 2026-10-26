// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lucindo/dynaflux/internal/archive"
)

// schemaProps are the properties of [TypeArchiveSchema].
type schemaProps struct {
	Database         string `json:"Database"`
	DatabaseLocation string `json:"DatabaseLocation"`
	WorkGroup        string `json:"WorkGroup"`
	OptimizerRole    string `json:"OptimizerRoleArn"`
	Optimizers       struct {
		Compaction      string `json:"Compaction"`
		RetentionDays   int    `json:"SnapshotRetentionDays,string"`
		SnapshotsToKeep int    `json:"SnapshotsToKeep,string"`
	} `json:"Optimizers"`
	Fingerprint string       `json:"Fingerprint"`
	Tables      []tableProps `json:"Tables"`
}

type tableProps struct {
	Name     string        `json:"Name"`
	Location string        `json:"Location"`
	Columns  []columnProps `json:"Columns"`
}

// columnProps converts to [archive.Column].
type columnProps struct {
	Name    string  `json:"Name"`
	Type    string  `json:"Type"`
	Default *string `json:"Default"`
}

// applySchema answers [TypeArchiveSchema]. It creates the database if it is missing, brings
// every table up to the properties, then puts the tables' optimizers, which need the
// tables, then re-creates their dedup views, which list the columns. Every step can run
// again. The physical ID is the database, so an update is never a replacement, and the
// attribute Fingerprint is the property's.
func (h *Handler) applySchema(ctx context.Context, props map[string]any) (Response, error) {
	var p schemaProps
	if err := decode(props, &p); err != nil {
		return Response{}, err
	}
	if err := p.check(); err != nil {
		return Response{}, err
	}
	if err := retry(ctx, func() error { return h.Glue.CreateDatabase(ctx, p.Database, p.DatabaseLocation) }); err != nil {
		return Response{}, fmt.Errorf("archive database %s: %w", p.Database, err)
	}
	for _, t := range p.Tables {
		if err := retry(ctx, func() error { return h.applyTable(ctx, p.Database, p.WorkGroup, t) }); err != nil {
			return Response{}, fmt.Errorf("archive table %s.%s: %w", p.Database, t.Name, err)
		}
	}
	for _, t := range p.Tables {
		for _, o := range p.optimizers() {
			if err := retry(ctx, func() error { return h.Glue.PutOptimizer(ctx, p.Database, t.Name, o) }); err != nil {
				return Response{}, fmt.Errorf("%s optimizer of archive table %s.%s: %w", o.Type, p.Database, t.Name, err)
			}
		}
	}
	for _, t := range p.Tables {
		view := archive.DedupView(p.Database, t.table())
		if err := retry(ctx, func() error { return h.Athena.Exec(ctx, p.WorkGroup, view) }); err != nil {
			return Response{}, fmt.Errorf("dedup view of archive table %s.%s: %w", p.Database, t.Name, err)
		}
	}
	return Response{PhysicalID: p.Database, Data: map[string]string{"Fingerprint": p.Fingerprint}}, nil
}

// applyTable creates t, or adds the columns that the live table lacks. A table read again
// after a commit conflict shows whether the columns were added.
func (h *Handler) applyTable(ctx context.Context, db, workgroup string, t tableProps) error {
	live, found, err := h.Glue.Table(ctx, db, t.Name)
	if err != nil {
		return err
	}
	if !found {
		return h.Athena.Exec(ctx, workgroup, archive.CreateTable(db, t.Location, t.table()))
	}
	missing, err := t.missing(live)
	if err != nil || len(missing) == 0 {
		return err
	}
	return h.Athena.Exec(ctx, workgroup, archive.AddColumns(db, t.table(), missing))
}

// check fails on properties the schema can't be applied from. A property the renderer
// renamed would otherwise read as empty, and no tables would mean success doing nothing.
// Anyone allowed to invoke the function can send properties, which become Athena
// statements, so the tables must have the form that the archive gives them.
func (p *schemaProps) check() error {
	var errs []error
	for _, f := range []struct{ name, value string }{
		{"Database", p.Database},
		{"DatabaseLocation", p.DatabaseLocation},
		{"WorkGroup", p.WorkGroup},
		{"OptimizerRoleArn", p.OptimizerRole},
		{"Optimizers.Compaction", p.Optimizers.Compaction},
		{"Fingerprint", p.Fingerprint},
	} {
		if f.value == "" {
			errs = append(errs, fmt.Errorf("property %s is missing", f.name))
		}
	}
	if len(p.Tables) == 0 {
		errs = append(errs, errors.New("property Tables is empty"))
	}
	for _, t := range p.Tables {
		switch err := t.table().Check(); {
		case err != nil:
			errs = append(errs, err)
		case len(t.Columns) == 0:
			errs = append(errs, fmt.Errorf("table %s has no columns", t.Name))
		// archive.CreateTable quotes the location.
		case !strings.HasPrefix(t.Location, "s3://") || strings.Contains(t.Location, "'"):
			errs = append(errs, fmt.Errorf("table %s: location %q isn't an S3 URI without quotes", t.Name, t.Location))
		}
	}
	return errors.Join(errs...)
}

func (p *schemaProps) optimizers() []Optimizer {
	return []Optimizer{
		{Type: OptimizerCompaction, Role: p.OptimizerRole, Strategy: p.Optimizers.Compaction},
		{
			Type: OptimizerRetention, Role: p.OptimizerRole,
			RetentionDays: p.Optimizers.RetentionDays, SnapshotsToKeep: p.Optimizers.SnapshotsToKeep,
		},
	}
}

func (t *tableProps) table() archive.Table {
	cols := make([]archive.Column, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = archive.Column(c)
	}
	return archive.Table{Name: t.Name, Columns: cols}
}

// missing returns t's columns that live lacks, in order. It fails when live is elsewhere,
// partitioned otherwise or not an Iceberg table, or when a column's type differs: fixing
// any of those would rewrite the archived events, so it is left to a person.
func (t *tableProps) missing(live GlueTable) ([]archive.Column, error) {
	switch {
	case !strings.EqualFold(live.TableType, "ICEBERG"):
		return nil, fmt.Errorf("it isn't an Iceberg table (table_type %q), and Dynaflux doesn't convert tables", live.TableType)
	case strings.TrimSuffix(live.Location, "/") != strings.TrimSuffix(t.Location, "/"):
		return nil, fmt.Errorf("it is at %s, not %s, and Dynaflux doesn't move tables", live.Location, t.Location)
	case live.Partition != archive.Partitioning:
		return nil, fmt.Errorf("its partition spec is (%s), not (%s), and Dynaflux doesn't repartition tables", live.Partition, archive.Partitioning)
	}
	var missing []archive.Column
	var differ []string
	for _, c := range t.Columns {
		typ, ok := live.Columns[c.Name]
		if !ok {
			missing = append(missing, archive.Column(c))
		} else if !sameType(typ, c.Type) {
			differ = append(differ, fmt.Sprintf("%s is %s, not %s", c.Name, typ, c.Type))
		}
	}
	if len(differ) > 0 {
		return nil, fmt.Errorf("column types differ, and Dynaflux doesn't change them: %s", strings.Join(differ, "; "))
	}
	return missing, nil
}

// sameType compares column types ignoring case and spaces, which Glue may spell otherwise
// than the DDL that created the column.
func sameType(a, b string) bool {
	norm := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, " ", "")) }
	return norm(a) == norm(b)
}
