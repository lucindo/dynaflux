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
	maxBucketName         = 63  // S3 general purpose bucket.
	maxLogGroupName       = 512 // CloudWatch Logs log group.
	maxStateMachineName   = 80  // Step Functions state machine.
	maxDeliveryStream     = 64  // Firehose stream.
	maxArchiveDatabase    = 252 // Iceberg database in Glue; S3 Tables namespaces allow 255.
	maxArchiveTableOrView = 255 // Iceberg table or Athena view.
	maxAthenaWorkGroup    = 128 // Athena workgroup.
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
	return n.Base() + "-stream-" + stream
}

// ViewTable names the regional table of a view version.
func (n Naming) ViewTable(view string, version int) string {
	return n.Base() + "-view-" + view + "-v" + strconv.Itoa(version)
}

// MetaTable names the store's regional meta table.
func (n Naming) MetaTable() string {
	return n.Base() + "-meta"
}

// ProjectorFunction names the store's projector function when runtime.split is store.
func (n Naming) ProjectorFunction() string {
	return n.Base() + "-projector"
}

// StreamProjectorFunction names a stream's projector function when runtime.split is stream.
func (n Naming) StreamProjectorFunction(stream string) string {
	return n.ProjectorFunction() + "-" + stream
}

// SetupFunction names the function that serves the templates' deploy-time requests.
func (n Naming) SetupFunction() string {
	return n.Base() + "-setup"
}

// Bucket names the store's S3 bucket in a region. The name is in the account regional
// namespace: only the account can create a name that ends in -{account}-{region}-an.
func (n Naming) Bucket(account, region string) string {
	return n.Base() + "-" + account + "-" + region + "-an"
}

// ValidPrefix reports whether s is a valid deployment prefix: "" for none, or
// [a-z][a-z0-9-]*- (config.md's prefix).
func ValidPrefix(s string) bool {
	if s == "" {
		return true
	}
	if s[0] < 'a' || s[0] > 'z' || s[len(s)-1] != '-' {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// ReservedBucketPrefixes returns the prefixes that S3 reserves at the start of bucket names.
func ReservedBucketPrefixes() []string {
	return []string{"xn--", "sthree-", "amzn-s3-demo-"}
}

// QueryFunction names the query function.
func (n Naming) QueryFunction() string {
	return n.Base() + "-query"
}

// BackfillFunction names the backfill worker function.
func (n Naming) BackfillFunction() string {
	return n.Base() + "-backfill"
}

// BackfillStateMachine names the backfill state machine.
func (n Naming) BackfillStateMachine() string {
	return n.Base() + "-backfill"
}

// ArchiveStream names the Firehose stream that archives a stream.
func (n Naming) ArchiveStream(stream string) string {
	return n.Base() + "-archive-" + stream
}

// ArchiveDatabase names the Glue database or S3 Tables namespace of the archive. Those
// names don't allow "-".
func (n Naming) ArchiveDatabase() string {
	return strings.ReplaceAll(n.Base(), "-", "_")
}

// ArchiveWorkGroup names the Athena workgroup that creates and evolves the archive tables.
func (n Naming) ArchiveWorkGroup() string {
	return n.Base()
}

// BackfillWorkGroup names the Athena workgroup in which backfills read the archive.
func (n Naming) BackfillWorkGroup() string {
	return n.Base() + "-backfill"
}

// ArchiveLogGroup names the log group of the Firehose streams.
func (n Naming) ArchiveLogGroup() string {
	return "/aws/kinesisfirehose/" + n.Base() + "-archive"
}

// StoreResources lists the AWS resource names that the prefix and store alone imply, with
// projectorPerStream set when runtime.split is stream. It includes the archive database
// even when no stream is archived.
func (n Naming) StoreResources(projectorPerStream bool) []Resource {
	res := []Resource{
		{Kind: ResourceMetaTable, Name: n.MetaTable()},
		{Kind: ResourceSetupFunction, Name: n.SetupFunction()},
		{Kind: ResourceQueryFunction, Name: n.QueryFunction()},
		{Kind: ResourceBackfillFunction, Name: n.BackfillFunction()},
		{Kind: ResourceBackfillStateMachine, Name: n.BackfillStateMachine()},
		{Kind: ResourceArchiveDatabase, Name: n.ArchiveDatabase()},
		{Kind: ResourceArchiveWorkGroup, Name: n.ArchiveWorkGroup()},
		{Kind: ResourceBackfillWorkGroup, Name: n.BackfillWorkGroup()},
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

// Base returns {prefix}{store}, which starts every name of the store and is the metrics'
// Store dimension.
func (n Naming) Base() string {
	return n.Prefix + n.Store
}

// FunctionLogGroup names the log group of a Lambda function.
func FunctionLogGroup(function string) string {
	return "/aws/lambda/" + function
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
	ResourceSetupFunction        ResourceKind = "setup function"
	ResourceQueryFunction        ResourceKind = "query function"
	ResourceBackfillFunction     ResourceKind = "backfill function"
	ResourceBackfillStateMachine ResourceKind = "backfill state machine"
	ResourceArchiveStream        ResourceKind = "Firehose stream"
	ResourceArchiveDatabase      ResourceKind = "archive database"
	ResourceArchiveWorkGroup     ResourceKind = "Athena workgroup"
	ResourceBackfillWorkGroup    ResourceKind = "backfill workgroup"
	ResourceIcebergTable         ResourceKind = "Iceberg table"
	ResourceDedupView            ResourceKind = "Athena view"
	ResourceBucket               ResourceKind = "S3 bucket"
	ResourceLogGroup             ResourceKind = "log group"
)

// MaxLen returns the longest name AWS allows for k.
func (k ResourceKind) MaxLen() int {
	switch k {
	case ResourceStreamTable, ResourceViewTable, ResourceMetaTable:
		return maxTableName
	case ResourceProjectorFunction, ResourceSetupFunction, ResourceQueryFunction, ResourceBackfillFunction:
		return maxFunctionName
	case ResourceBackfillStateMachine:
		return maxStateMachineName
	case ResourceArchiveStream:
		return maxDeliveryStream
	case ResourceArchiveDatabase:
		return maxArchiveDatabase
	case ResourceArchiveWorkGroup, ResourceBackfillWorkGroup:
		return maxAthenaWorkGroup
	case ResourceIcebergTable, ResourceDedupView:
		return maxArchiveTableOrView
	case ResourceBucket:
		return maxBucketName
	case ResourceLogGroup:
		return maxLogGroupName
	}
	return 0
}
