// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"strconv"
	"strings"
)

// Longest names AWS allows. Names are ASCII, so bytes and characters agree.
const (
	maxTableName          = 255 // DynamoDB table.
	maxFunctionName       = 64  // Lambda function.
	maxStateMachineName   = 80  // Step Functions state machine.
	maxDeliveryStream     = 64  // Firehose stream.
	maxArchiveDatabase    = 252 // Iceberg database in Glue; S3 Tables namespaces allow 255.
	maxArchiveTableOrView = 255 // Iceberg table or Athena view.
)

// Naming derives AWS resource names, {prefix}{store}-{kind}-{name}, from logical names.
// The prefix names a deployment environment, so it isn't in the catalog: the runtime reads
// it from [EnvPrefix].
type Naming struct {
	Prefix string
	Store  string
}

// StreamTable names a stream's Global Table.
func (n Naming) StreamTable(stream string) string {
	return n.base() + "-stream-" + stream
}

// ViewTable names the regional table of a view version.
func (n Naming) ViewTable(view string, version int) string {
	return n.base() + "-view-" + view + "-v" + strconv.Itoa(version)
}

// MetaTable names the store's regional meta table.
func (n Naming) MetaTable() string {
	return n.base() + "-meta"
}

// ProjectorFunction names the store's projector function when runtime.split is store.
func (n Naming) ProjectorFunction() string {
	return n.base() + "-projector"
}

// StreamProjectorFunction names a stream's projector function when runtime.split is stream.
func (n Naming) StreamProjectorFunction(stream string) string {
	return n.ProjectorFunction() + "-" + stream
}

// QueryFunction names the query function.
func (n Naming) QueryFunction() string {
	return n.base() + "-query"
}

// BackfillFunction names the backfill worker function.
func (n Naming) BackfillFunction() string {
	return n.base() + "-backfill"
}

// BackfillStateMachine names the backfill state machine.
func (n Naming) BackfillStateMachine() string {
	return n.base() + "-backfill"
}

// ArchiveStream names the Firehose stream that archives a stream.
func (n Naming) ArchiveStream(stream string) string {
	return n.base() + "-archive-" + stream
}

// ArchiveDatabase names the Glue database or S3 Tables namespace of the archive. Those
// names don't allow "-".
func (n Naming) ArchiveDatabase() string {
	return strings.ReplaceAll(n.base(), "-", "_")
}

// StoreResources lists the AWS resource names that the prefix and store alone imply, with
// projectorPerStream set when runtime.split is stream. It includes the archive database
// even when no stream is archived.
func (n Naming) StoreResources(projectorPerStream bool) []Resource {
	res := []Resource{
		{Kind: ResourceMetaTable, Name: n.MetaTable()},
		{Kind: ResourceQueryFunction, Name: n.QueryFunction()},
		{Kind: ResourceBackfillFunction, Name: n.BackfillFunction()},
		{Kind: ResourceBackfillStateMachine, Name: n.BackfillStateMachine()},
		{Kind: ResourceArchiveDatabase, Name: n.ArchiveDatabase()},
	}
	if !projectorPerStream {
		res = append(res, Resource{Kind: ResourceProjectorFunction, Name: n.ProjectorFunction()})
	}
	return res
}

// CatalogResources lists the AWS resource names that derive from the streams and views of
// c, with projectorPerStream set when runtime.split is stream.
func (n Naming) CatalogResources(c *Catalog, projectorPerStream bool) []Resource {
	var res []Resource
	for _, s := range c.Streams {
		res = append(res, Resource{Kind: ResourceStreamTable, Owner: s.Name, Name: n.StreamTable(s.Name)})
		if projectorPerStream {
			res = append(res, Resource{Kind: ResourceProjectorFunction, Owner: s.Name, Name: n.StreamProjectorFunction(s.Name)})
		}
		if s.Archive != nil {
			res = append(res,
				Resource{Kind: ResourceArchiveStream, Owner: s.Name, Name: n.ArchiveStream(s.Name)},
				Resource{Kind: ResourceIcebergTable, Owner: s.Name, Name: IcebergTable(s.Name)},
				Resource{Kind: ResourceDedupView, Owner: s.Name, Name: DedupView(s.Name)},
			)
		}
	}
	for _, v := range c.Views {
		for _, vv := range v.Versions {
			res = append(res, Resource{Kind: ResourceViewTable, Owner: v.Name, Name: n.ViewTable(v.Name, vv.Number)})
		}
	}
	return res
}

func (n Naming) base() string {
	return n.Prefix + n.Store
}

// IcebergTable names a stream's archive table.
func IcebergTable(stream string) string {
	return stream
}

// DedupView names the Athena view with one row per event of a stream's archive.
func DedupView(stream string) string {
	return stream + "_dedup"
}

// Resource is an AWS resource name that Dynaflux derives.
type Resource struct {
	Kind ResourceKind
	// Owner is the stream or view the name derives from; empty for store-level resources.
	Owner string
	Name  string
}

// TooLong reports whether Name is longer than AWS allows for its kind.
func (r Resource) TooLong() bool {
	return len(r.Name) > r.Kind.MaxLen()
}

// ResourceKind is a kind of AWS resource that Dynaflux names.
type ResourceKind string

// Resource kinds.
const (
	ResourceStreamTable          ResourceKind = "stream table"
	ResourceViewTable            ResourceKind = "view table"
	ResourceMetaTable            ResourceKind = "meta table"
	ResourceProjectorFunction    ResourceKind = "projector function"
	ResourceQueryFunction        ResourceKind = "query function"
	ResourceBackfillFunction     ResourceKind = "backfill function"
	ResourceBackfillStateMachine ResourceKind = "backfill state machine"
	ResourceArchiveStream        ResourceKind = "Firehose stream"
	ResourceArchiveDatabase      ResourceKind = "archive database"
	ResourceIcebergTable         ResourceKind = "Iceberg table"
	ResourceDedupView            ResourceKind = "Athena view"
)

// MaxLen returns the longest name AWS allows for k.
func (k ResourceKind) MaxLen() int {
	switch k {
	case ResourceStreamTable, ResourceViewTable, ResourceMetaTable:
		return maxTableName
	case ResourceProjectorFunction, ResourceQueryFunction, ResourceBackfillFunction:
		return maxFunctionName
	case ResourceBackfillStateMachine:
		return maxStateMachineName
	case ResourceArchiveStream:
		return maxDeliveryStream
	case ResourceArchiveDatabase:
		return maxArchiveDatabase
	case ResourceIcebergTable, ResourceDedupView:
		return maxArchiveTableOrView
	}
	return 0
}
