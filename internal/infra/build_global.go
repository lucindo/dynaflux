// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import "github.com/lucindo/dynaflux/internal/catalog"

// global returns the global scope: one Global Table per stream, which holds the facts, so
// it is retained, protected from deletion and from replacement, and recoverable.
func (b *builder) global() Scope {
	tables := make([]Table, 0, len(b.cat.Streams))
	for _, s := range b.cat.Streams {
		tables = append(tables, Table{
			Common:              Common{ID: streamID(s.Name), Tags: b.tags(TagKindStream, s.Name)},
			Name:                b.name(catalog.ResourceStreamTable, b.naming.StreamTable(s.Name)),
			Role:                TableStream,
			Keys:                tableKeys(),
			TTLAttribute:        catalog.AttrTTL,
			Replicas:            b.regions,
			Consistency:         ConsistencyEventual,
			StreamView:          StreamViewNewAndOld,
			Capacity:            b.tableCapacity(s.Name),
			Deletion:            DeletionRetain,
			DeletionProtection:  true,
			UnprotectedRegion:   b.opts.UnprotectRegion,
			PointInTimeRecovery: true,
			NoReplace:           true,
		})
	}
	sortByID(tables)
	return Scope{Tables: tables}
}
