// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"cmp"
	"slices"

	"github.com/lucindo/dynaflux/internal/archive"
)

// Manifest describes the AWS resources of one store.
type Manifest struct {
	Store   string
	Regions []string // sorted
	Prefix  Prefix
	// ArchiveRegions, sorted, is set when only some of Regions archive. The resources and
	// statements marked archive-only then exist only there; otherwise they exist everywhere.
	ArchiveRegions []string
	// Global is deployed once, in any one region of Regions; Regional in each of them.
	Global   Scope
	Regional Scope
}

// Prefix is the deployment prefix of every name, chosen at deploy time.
type Prefix struct {
	// Default is the prefix in dynaflux.yaml, possibly empty.
	Default string
	// MaxLen is the longest prefix with which every name in the manifest stays within its
	// AWS limit, in every region of the store and with a 12-digit account ID.
	MaxLen int
}

// Scope lists the resources of one scope by kind, each list sorted by ID.
type Scope struct {
	Tables            []Table
	Buckets           []Bucket
	LogGroups         []LogGroup
	LogStreams        []LogStream
	Roles             []Role
	Functions         []Function
	StreamLookups     []StreamLookup
	Mappings          []Mapping
	Catalogs          []CatalogRegistration
	StateMachines     []StateMachine
	ArchiveWorkGroups []ArchiveWorkGroup
	ArchiveSchemas    []ArchiveSchema
	DeliveryStreams   []DeliveryStream
	Alarms            []Alarm
	Dashboards        []Dashboard
}

// Common holds the fields of every resource.
type Common struct {
	ID string
	// ArchiveOnly marks a resource that exists only in the regions that archive, which
	// Manifest.ArchiveRegions lists.
	ArchiveOnly bool
	// Tags are the dynaflux:* tags. They never hold the Dynaflux release, which would
	// update every resource on every upgrade.
	Tags map[string]string
}

func (c Common) common() Common {
	return c
}

func sortByID[T interface{ common() Common }](s []T) {
	slices.SortFunc(s, func(x, y T) int { return cmp.Compare(x.common().ID, y.common().ID) })
}

// Tag keys and the values of [TagKind].
const (
	TagStore = "dynaflux:store"
	TagKind  = "dynaflux:kind"
	TagName  = "dynaflux:name"
	// TagVersion and TagFingerprint are on view tables: the version's number and its
	// fingerprint in dynaflux.lock, both fixed for the version.
	TagVersion     = "dynaflux:version"
	TagFingerprint = "dynaflux:fingerprint"

	TagKindStream    = "stream"
	TagKindView      = "view"
	TagKindMeta      = "meta"
	TagKindBucket    = "bucket"
	TagKindProjector = "projector"
	TagKindSetup     = "setup"
	TagKindArchive   = "archive"
	TagKindBackfill  = "backfill"
)

// Table is a DynamoDB table. A stream table is a Global Table with a replica in every
// region of the store; its ID is "Stream" and the stream's name part. The meta table, "Meta",
// and the view tables, "View" with the view's name part, "V" and the version number, are
// regional.
type Table struct {
	Common

	Name Text
	Role TableRole
	Keys []Key
	// TTLAttribute is on every table: items without it never expire, and turning TTL on
	// and off would fail a quick redeploy, since DynamoDB allows one change per hour.
	TTLAttribute string
	// Replicas, Consistency and StreamView are set on stream tables only.
	Replicas    []string
	Consistency string
	StreamView  string
	Capacity    Capacity
	Deletion    Deletion
	// DeletionProtection and PointInTimeRecovery apply to every replica of a stream table,
	// except that the replica in UnprotectedRegion has no deletion protection: turning it
	// off is the first step of removing a region.
	DeletionProtection  bool
	UnprotectedRegion   string
	PointInTimeRecovery bool
	// NoReplace forbids an update that replaces the table: a replaced stream table starts
	// empty while the retained one keeps the facts.
	NoReplace bool
}

// TableRole is what a table holds.
type TableRole string

// Table roles.
const (
	TableStream TableRole = "stream"
	TableView   TableRole = "view"
	TableMeta   TableRole = "meta"
)

// Fixed values of stream tables.
const (
	// ConsistencyEventual is multi-region eventual consistency, which can't change after
	// the table is created.
	ConsistencyEventual = "eventual"
	// StreamViewNewAndOld lets the projector compare both images of a MODIFY record.
	StreamViewNewAndOld = "new_and_old_images"
)

// Key is a key attribute of a table.
type Key struct {
	Name string
	Type string // a DynamoDB attribute type
	Key  KeyRole
}

// KeyRole is a key attribute's role.
type KeyRole string

// Key roles.
const (
	KeyHash  KeyRole = "hash"
	KeyRange KeyRole = "range"
)

// Capacity is how a table is billed and scaled. Read and Write are set when Mode is
// provisioned.
type Capacity struct {
	Mode  CapacityMode
	Read  *Scaling
	Write *Scaling
}

// CapacityMode is a DynamoDB billing mode.
type CapacityMode string

// Capacity modes.
const (
	CapacityOnDemand    CapacityMode = "on_demand"
	CapacityProvisioned CapacityMode = "provisioned"
)

// Scaling is the auto scaling range of read or write capacity units in each region.
type Scaling struct {
	Min           int
	Max           int
	TargetPercent int
}

// Deletion is what happens to a resource when it leaves the template or its stack is
// deleted.
type Deletion string

// Deletion behaviors.
const (
	DeletionDelete Deletion = "delete"
	DeletionRetain Deletion = "retain"
	// DeletionRetainExceptOnCreate keeps the resource, except when the create that made it
	// rolls back, so that a failed first deploy can be retried as it is.
	DeletionRetainExceptOnCreate Deletion = "retain_except_on_create"
)

// Bucket is the store's S3 bucket in a region, "Bucket". Its name is in the account
// regional namespace, so no other account can take it. It is encrypted with S3 managed
// keys, disables ACLs, blocks public access, refuses requests without TLS, and keeps
// versions: it holds the archive, the only copy of old events.
type Bucket struct {
	Common

	Name Text
	// NoncurrentExpiryDays is how long a replaced or deleted object's version stays. The
	// bucket never expires current objects.
	NoncurrentExpiryDays int
	AbortMultipartDays   int
	// Expiries delete what lies under their prefixes sooner.
	Expiries []Expiry
	Deletion Deletion
}

// Expiry deletes the objects under Prefix Days after they were written, and their older
// versions NoncurrentDays after they were replaced or deleted.
type Expiry struct {
	ID             string
	Prefix         string
	Days           int
	NoncurrentDays int
}

// LogGroup is a CloudWatch Logs log group, deleted with its stack. Its ID is "Logs" and the
// ID of the resource that writes to it.
type LogGroup struct {
	Common

	Name          Text
	RetentionDays int
}

// LogStream is a log stream that a writer needs to exist, such as a Firehose stream's:
// "LogStream" and the stream's name part.
type LogStream struct {
	Common

	Group Text
	Name  string
}

// Role is an IAM role with inline policies, which exist before anything that needs them.
// Its ID is "Role" and the ID of the resource that assumes it. Roles have no name: IAM names
// are global to the account, and the regional scope is deployed in every region.
type Role struct {
	Common

	// Service is the service principal that may assume the role, when every condition of
	// Trust holds. Trust has a condition only where the service documents one.
	Service    string
	Trust      []Condition
	Statements []Statement
	// Managed are managed policies attached to the role, for statements that grow with the
	// store past what its inline policies may hold. Each fits IAM's limit on one managed
	// policy.
	Managed []ManagedPolicy
}

// ManagedPolicy is an unnamed IAM managed policy attached to the role that holds it: the
// role's ID, "Policy" and its number from 1.
type ManagedPolicy struct {
	ID         string
	Statements []Statement
}

// Service principals.
const (
	ServiceLambda   = "lambda.amazonaws.com"
	ServiceFirehose = "firehose.amazonaws.com"
	ServiceGlue     = "glue.amazonaws.com"
	ServiceStates   = "states.amazonaws.com"
)

// Statement allows Actions, sorted, on Resources, when every condition holds. A resource is
// "*" only for an action that has no resource type.
type Statement struct {
	Sid string
	// ArchiveOnly marks a statement that exists only in the regions that archive. A statement
	// of a role that exists everywhere must be one when it refers to an archive-only resource.
	ArchiveOnly bool
	Actions     []string
	Resources   []Text
	Conditions  []Condition
}

// Condition holds when the request's Key matches one of Values under Operator, an IAM
// condition operator such as StringEquals.
type Condition struct {
	Operator string
	Key      string
	Values   []Text
}

// Function is a Lambda function that runs the store's runtime binary. Every function runs
// the same binary, and its Mode tells the binary what to do.
type Function struct {
	Common

	Name Text
	// Mode is the value of catalog.EnvMode.
	Mode           string
	Role           Text
	LogGroup       Text
	Runtime        string
	Handler        string
	Arch           string
	MemoryMB       int
	TimeoutSeconds int
	// Code is the directory of the binary's main package, relative to infra.out, where the
	// templates are.
	Code string
	Env  map[string]Text
}

// Fixed values of functions.
const (
	FunctionRuntime = "provided.al2023"
	FunctionHandler = "bootstrap"
)

// StreamLookup finds the stream ARN of a stream table's replica in the region where the
// scope is deployed, which only a request to DynamoDB in that region gives: "Lookup" and
// the stream's name part. Handler, the setup function, answers it, and fails while the
// replica isn't ready (ADR 0008).
type StreamLookup struct {
	Common

	Handler Text
	Table   Text
}

// Mapping is an event source mapping from the local replica's stream of a stream table to
// the stream's projector: one per stream in each region. Its ID is "Mapping" and the
// stream's name part, whatever function it calls, so that a change of runtime.split updates
// it in place instead of putting a second reader on the stream.
type Mapping struct {
	Common

	Function  Text
	Source    Text
	OnFailure Text
	// StartingPosition and the other fields are the fixed values of docs/spec/infra.md §8.
	StartingPosition        string
	BatchSize               int
	BatchingWindowSeconds   int
	ParallelizationFactor   int
	BisectOnError           bool
	ReportBatchItemFailures bool
	MaxRetryAttempts        int
	MaxRecordAgeSeconds     int
	TumblingWindowSeconds   int
	Metrics                 []string
}

// Fixed values of mappings.
const (
	// StartingTrimHorizon starts a new mapping at the oldest record: starting at the latest
	// can miss records while a mapping is created or updated.
	StartingTrimHorizon = "trim_horizon"
	// MetricEventCount turns on the mapping's event counts, which the alarms watch.
	MetricEventCount = "event_count"
)

// StateMachine is the Standard Step Functions state machine that drives each backfill job in
// the region, "BackfillStateMachine" (ADR 0011). An execution invokes Function one step at a
// time and waits as a step asks; after MaxSteps steps it starts its continuation and ends,
// well before an execution's history fills. It logs errors, without execution data, to
// LogGroup.
type StateMachine struct {
	Common

	Name     Text
	Role     Text
	LogGroup Text // the log group's ARN
	Function Text // the function's ARN
	MaxSteps int
}

// CatalogRegistration is "Catalog", which the setup function answers by registering the
// catalog of the runtime binary in the meta table, and by setting retired the versions that
// the catalog lists retired (ADR 0013). Its physical ID changes with Hash, so a new catalog
// replaces it, and CloudFormation deletes the old one only after the whole update
// succeeded. CloudFormation can't tag it.
type CatalogRegistration struct {
	Common

	Handler   Text
	MetaTable Text
	// Hash is the lowercase hex SHA-256 of the catalog's bytes, which the runtime binary
	// embeds.
	Hash string
	// After lists the IDs of the functions it is registered after, besides Handler: the
	// catalog becomes current once every function runs it.
	After []string
}

// ArchiveWorkGroup is the Athena workgroup that runs the archive schema's statements and the
// users' queries on the archive, "ArchiveWorkGroup". It uses engine version 3 and managed
// query results, and enforces its configuration. It is deleted with its query history and
// saved queries: Athena refuses to delete a workgroup that has run any query otherwise.
type ArchiveWorkGroup struct {
	Common

	Name Text
}

// ArchiveSchema is the desired state of the archive in a region, "ArchiveSchema", which the
// setup function applies (ADR 0008). It creates the Glue database and the tables that are
// missing, adds the columns a table lacks, keeps the table optimizers, and replaces each
// table's dedup view. It never moves, repartitions or drops anything, and the database and
// tables outlive the stack, so a new stack or an archive turned back on finds them as they
// were. CloudFormation can't tag it.
type ArchiveSchema struct {
	Common

	// Handler is the function that applies the schema.
	Handler Text
	// Database is the Glue database of the tables, at DatabaseLocation under warehouse/ in
	// the bucket.
	Database         Text
	DatabaseLocation Text
	WorkGroup        Text
	Tables           []ArchiveTable
	Optimizers       Optimizers
	// After lists the IDs of the resources it is applied after: the stream lookups, so that
	// a missing replica fails the deploy before anything is written.
	After []string
	// Fingerprint is the tables' [archive.Schema.Fingerprint], which doesn't depend on the
	// prefix. Projectors compare it with the one their catalog gives.
	Fingerprint string
	Deletion    Deletion
}

// ArchiveTable is the Iceberg table of an archived stream. The setup function writes its
// dedup view from the columns and their defaults.
type ArchiveTable struct {
	Name string
	// Location ends in "/iceberg/", so no table's location is a key prefix of another's,
	// as page's would be of page_views'.
	Location Text
	Columns  []archive.Column
}

// Optimizers are the Glue table optimizers of every archive table, which Glue runs as Role:
// compaction, and snapshot retention, which removes snapshots older than
// SnapshotRetentionDays, keeping at least SnapshotsToKeep, with their expired files.
type Optimizers struct {
	Role                  Text
	Compaction            string
	SnapshotRetentionDays int
	SnapshotsToKeep       int
}

// CompactionBinpack compacts small files into larger ones.
const CompactionBinpack = "binpack"

// DeliveryStream is the Direct PUT Firehose stream that writes a stream's events into its
// archive table, "Firehose" and the stream's name part. Its name, type, AppendOnly and
// catalog can't change after it is created, and they derive from the stream name alone: a
// replaced delivery stream would drop the records it buffers. Firehose writes records that
// it fails to deliver to the bucket, under ErrorPrefix.
type DeliveryStream struct {
	Common

	Name Text
	// Schema is the ID of the archive schema that holds its table: it is created after it.
	Schema string
	// Catalog is the ARN of the Glue Data Catalog of Database.
	Catalog         Text
	Database        Text
	Table           string
	Role            Text
	Bucket          Text
	ErrorPrefix     string
	LogGroup        Text
	LogStream       Text
	AppendOnly      bool
	BufferSeconds   int
	BufferMegabytes int
	RetrySeconds    int
}

// Alarm is a CloudWatch alarm with a fixed rule: it fires when the sum of its metrics,
// each taken over PeriodSeconds, compares to Threshold as Comparison says for
// EvaluationPeriods periods in a row. Its ID is "Alarm", the signal and the ID of the
// resource it watches, or "Alarm" and the signal for an alarm on the whole store. Where it
// notifies is the renderer's choice, made at deploy time.
type Alarm struct {
	Common

	Name     Text
	Severity Severity
	// Description starts with the severity and a colon, and says what to do.
	Description       string
	Metrics           []Metric
	PeriodSeconds     int
	EvaluationPeriods int
	Threshold         int
	Comparison        Comparison
	TreatMissing      TreatMissing
}

// Severity is how urgent an alarm is.
type Severity string

// Severities.
const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
)

// Comparison is how an alarm compares its metric with its threshold.
type Comparison string

// Comparisons.
const (
	ComparisonAtLeast Comparison = "at_least"
	ComparisonBelow   Comparison = "below"
)

// TreatMissing is how an alarm treats a period without data.
type TreatMissing string

// Treatments of missing data.
const (
	MissingNotBreaching TreatMissing = "not_breaching"
	MissingBreaching    TreatMissing = "breaching"
)

// Metric is a CloudWatch metric and the statistic an alarm takes of it.
type Metric struct {
	Namespace  string
	Name       string
	Dimensions []Dimension
	Statistic  string
}

// Dimension selects the series of a metric.
type Dimension struct {
	Name  string
	Value Text
}

// Dashboard is the store's CloudWatch dashboard in a region, "Dashboard". Dashboard names
// are global to the account, so its name holds the region. CloudFormation can't tag it.
type Dashboard struct {
	Common

	Name Text
	// Body is the dashboard's JSON. CloudWatch needs the region of every metric widget, and
	// alarm widgets list alarm ARNs, so it holds no reference: only ${prefix}, ${region},
	// ${account} and ${partition}.
	Body Text
}
