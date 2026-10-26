// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"maps"
	"slices"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
)

// ProjectorBatchSize is the largest batch that a mapping hands to the projector. dynaflux
// explain assumes the same size.
const ProjectorBatchSize = 100

// Fixed values of docs/spec/infra.md §8.
const (
	// tumblingWindowSeconds only tells the projector its shard (ADR 0003).
	tumblingWindowSeconds = 900
	// unlimited retry attempts and record age: a mapping never discards a record it can
	// still read, since a discarded batch leaves a hole in the views.
	unlimited            = -1
	logRetentionDays     = 30
	setupTimeoutSeconds  = 900
	noncurrentExpiryDays = 30
	abortMultipartDays   = 7
)

// IAM names that several statements use.
const (
	sidLogs            = "Logs"
	condStringEquals   = "StringEquals"
	s3GetObject        = "s3:GetObject"
	s3PutObject        = "s3:PutObject"
	s3ListBucket       = "s3:ListBucket"
	logsPutLogEvents   = "logs:PutLogEvents"
	glueCreateDatabase = "glue:CreateDatabase"
	glueGetDatabase    = "glue:GetDatabase"
	glueGetTable       = "glue:GetTable"
	glueUpdateTable    = "glue:UpdateTable"
)

// regional returns the regional scope: the meta table and a table per view version, the
// bucket, the projectors and the setup function with their log groups and roles, a lookup
// and a mapping per stream, the archive, and the alarms that watch them.
func (b *builder) regional() Scope {
	s := Scope{Tables: b.regionalTables(), Buckets: []Bucket{b.bucket()}}
	if b.cfg.Runtime.Split == config.SplitStream {
		for _, st := range b.cat.Streams {
			b.addFunction(&s, b.projector(streamProjectorID(st.Name), b.naming.StreamProjectorFunction(st.Name), st.Name, st))
		}
	} else {
		b.addFunction(&s, b.projector(idProjector, b.naming.ProjectorFunction(), "", b.cat.Streams...))
	}
	b.addFunction(&s, b.setup())
	for _, st := range b.cat.Streams {
		s.StreamLookups = append(s.StreamLookups, StreamLookup{
			Common: Common{ID: lookupID(st.Name)}, Handler: ref(idSetup, AttrARN), Table: ref(streamID(st.Name), AttrName),
		})
		s.Mappings = append(s.Mappings, b.mapping(st.Name))
	}
	if b.archives() {
		b.addArchive(&s)
	}
	sortByID(s.LogGroups)
	sortByID(s.LogStreams)
	sortByID(s.Roles)
	sortByID(s.Functions)
	sortByID(s.StreamLookups)
	sortByID(s.Mappings)
	sortByID(s.DeliveryStreams)
	s.Alarms = b.alarms(&s)
	return s
}

// regionalTables returns the meta table and the table of every version in the catalog,
// current and retained. They are derived state that lives and dies together: a meta table
// that outlived its view tables would mark new, empty ones live and skip their records.
func (b *builder) regionalTables() []Table {
	table := func(id string, name Text, role TableRole, c Capacity, tags map[string]string) Table {
		return Table{
			Common: Common{ID: id, Tags: tags}, Name: name, Role: role, Keys: tableKeys(),
			TTLAttribute: catalog.AttrTTL, Capacity: c, Deletion: DeletionDelete,
		}
	}
	tables := []Table{table(idMeta, b.name(catalog.ResourceMetaTable, b.naming.MetaTable()), TableMeta,
		capacity(b.cfg.Defaults.Capacity), b.tags(TagKindMeta, ""))}
	for _, v := range b.cat.Views {
		for _, vv := range v.Versions {
			tags := b.tags(TagKindView, v.Name)
			tags[TagVersion] = strconv.Itoa(vv.Number)
			tags[TagFingerprint] = vv.Fingerprint
			tables = append(tables, table(viewID(v.Name, vv.Number),
				b.name(catalog.ResourceViewTable, b.naming.ViewTable(v.Name, vv.Number)), TableView, b.tableCapacity(v.Name), tags))
		}
	}
	sortByID(tables)
	return tables
}

func (b *builder) bucket() Bucket {
	return Bucket{
		Common: Common{ID: idBucket, Tags: b.tags(TagKindBucket, "")},
		Name: b.name(catalog.ResourceBucket,
			b.naming.Bucket(placeholder(PlaceholderAccount), placeholder(PlaceholderRegion))),
		NoncurrentExpiryDays: noncurrentExpiryDays,
		AbortMultipartDays:   abortMultipartDays,
		Deletion:             DeletionRetainExceptOnCreate,
	}
}

// function is a function to add with its log group and role.
type function struct {
	id             string
	kind           catalog.ResourceKind
	name           string
	mode           string
	tags           map[string]string
	timeoutSeconds int
	// env holds the environment variables besides those of every function.
	env map[string]Text
	// statements are the role's statements besides the one for the log group.
	statements []Statement
}

// addFunction adds f to s, with a log group that only it writes and a role that only it
// assumes.
func (b *builder) addFunction(s *Scope, f function) {
	name := b.name(f.kind, f.name)
	logs := LogGroup{
		Common:        Common{ID: logsID(f.id), Tags: maps.Clone(f.tags)},
		Name:          b.name(catalog.ResourceLogGroup, catalog.FunctionLogGroup(string(name))),
		RetentionDays: logRetentionDays,
	}
	writeLogs := Statement{
		Sid: sidLogs, Actions: []string{"logs:CreateLogStream", logsPutLogEvents},
		Resources: []Text{ref(logs.ID, AttrARN)},
	}
	role := Role{
		Common:     Common{ID: roleID(f.id), Tags: maps.Clone(f.tags)},
		Service:    ServiceLambda,
		Statements: append([]Statement{writeLogs}, f.statements...),
	}
	env := map[string]Text{
		catalog.EnvMode:   Text(f.mode),
		catalog.EnvStore:  Text(b.cat.Store),
		catalog.EnvPrefix: Text(placeholder(PlaceholderPrefix)),
	}
	maps.Copy(env, f.env)
	s.LogGroups = append(s.LogGroups, logs)
	s.Roles = append(s.Roles, role)
	s.Functions = append(s.Functions, Function{
		Common:         Common{ID: f.id, Tags: f.tags},
		Name:           name,
		Mode:           f.mode,
		Role:           ref(role.ID, AttrARN),
		LogGroup:       ref(logs.ID, AttrName),
		Runtime:        FunctionRuntime,
		Handler:        FunctionHandler,
		Arch:           string(b.cfg.Runtime.Arch),
		MemoryMB:       b.cfg.Runtime.MemoryMB,
		TimeoutSeconds: f.timeoutSeconds,
		Code:           b.code,
		Env:            env,
	})
}

// projector returns a projector of streams: all of them when runtime.split is store, one
// when it is stream, which owner then names. Its role reads only those streams, writes only
// the tables of the view versions that read them, and archives only those streams.
func (b *builder) projector(id, name, owner string, streams ...catalog.Stream) function {
	reads := map[string]bool{}
	var streamARNs, viewARNs, archiveARNs []Text
	for _, s := range streams {
		reads[s.Name] = true
		streamARNs = append(streamARNs, ref(streamID(s.Name), AttrStreamARNPattern))
		if s.Archive != nil {
			archiveARNs = append(archiveARNs, ref(deliveryStreamID(s.Name), AttrARN))
		}
	}
	for _, v := range b.cat.Views {
		for _, vv := range v.Versions {
			if reads[vv.Stream] {
				viewARNs = append(viewARNs, ref(viewID(v.Name, vv.Number), AttrARN))
			}
		}
	}
	slices.Sort(streamARNs)
	slices.Sort(viewARNs)
	slices.Sort(archiveARNs)
	statements := []Statement{
		{Sid: "ReadStreams", Actions: []string{"dynamodb:DescribeStream", "dynamodb:GetRecords", "dynamodb:GetShardIterator"}, Resources: streamARNs},
		{Sid: "ListStreams", Actions: []string{"dynamodb:ListStreams"}, Resources: []Text{"*"}},
		{Sid: "Meta", Actions: []string{"dynamodb:BatchGetItem", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query"}, Resources: []Text{ref(idMeta, AttrARN)}},
	}
	if len(viewARNs) > 0 {
		statements = append(statements, Statement{Sid: "Views", Actions: []string{"dynamodb:BatchGetItem", "dynamodb:GetItem", "dynamodb:PutItem"}, Resources: viewARNs})
	}
	if len(archiveARNs) > 0 {
		statements = append(statements, Statement{Sid: "Archive", ArchiveOnly: b.archiveOnly, Actions: []string{"firehose:PutRecordBatch"}, Resources: archiveARNs})
	}
	// Every projector checks the archive schema, since its catalog holds every stream.
	var env map[string]Text
	if b.archives() {
		env = map[string]Text{catalog.EnvArchiveSchema: ref(idArchiveSchema, AttrFingerprint)}
	}
	// Lambda writes what a mapping discards to the bucket with these grants.
	ownBucket := []Condition{{Operator: condStringEquals, Key: "s3:ResourceAccount", Values: []Text{Text(placeholder(PlaceholderAccount))}}}
	statements = append(statements,
		Statement{Sid: "FailureList", Actions: []string{s3ListBucket}, Resources: []Text{ref(idBucket, AttrARN)}, Conditions: ownBucket},
		Statement{Sid: "FailurePut", Actions: []string{s3PutObject}, Resources: []Text{ref(idBucket, AttrARN) + "/aws/lambda/*"}, Conditions: ownBucket},
	)
	return function{
		id: id, kind: catalog.ResourceProjectorFunction, name: name, mode: catalog.EnvModeProjector,
		tags: b.tags(TagKindProjector, owner), timeoutSeconds: b.cfg.Runtime.TimeoutSeconds, env: env, statements: statements,
	}
}

// setup returns the function that serves the templates' deploy-time requests (ADR 0008). It
// never writes view rows, so its timeout isn't bound by runtime.prune_window_seconds.
func (b *builder) setup() function {
	var tables []Text
	for _, s := range b.cat.Streams {
		tables = append(tables, ref(streamID(s.Name), AttrARN))
	}
	slices.Sort(tables)
	statements := []Statement{{Sid: "StreamTables", Actions: []string{"dynamodb:DescribeTable"}, Resources: tables}}
	if b.archives() {
		statements = append(statements, b.setupArchiveStatements()...)
	}
	return function{
		id: idSetup, kind: catalog.ResourceSetupFunction, name: b.naming.SetupFunction(), mode: catalog.EnvModeSetup,
		tags: b.tags(TagKindSetup, ""), timeoutSeconds: setupTimeoutSeconds, statements: statements,
	}
}

// mapping returns the mapping from the local replica of a stream's table to its projector.
func (b *builder) mapping(stream string) Mapping {
	projector := idProjector
	if b.cfg.Runtime.Split == config.SplitStream {
		projector = streamProjectorID(stream)
	}
	return Mapping{
		Common:                  Common{ID: mappingID(stream)},
		Function:                ref(projector, AttrName),
		Source:                  ref(lookupID(stream), AttrStreamARN),
		OnFailure:               ref(idBucket, AttrARN),
		StartingPosition:        StartingTrimHorizon,
		BatchSize:               ProjectorBatchSize,
		BatchingWindowSeconds:   b.cfg.Runtime.BatchingWindowSeconds,
		ParallelizationFactor:   1,
		BisectOnError:           true,
		ReportBatchItemFailures: true,
		MaxRetryAttempts:        unlimited,
		MaxRecordAgeSeconds:     unlimited,
		TumblingWindowSeconds:   tumblingWindowSeconds,
		Metrics:                 []string{MetricEventCount},
	}
}
