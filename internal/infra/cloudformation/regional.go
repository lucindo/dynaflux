// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
	"github.com/lucindo/dynaflux/internal/setup"
)

// Service timeouts of the custom resources, so that a setup function that never replies
// fails the deploy in minutes rather than an hour. The archive schema's Athena statements
// can take minutes; its timeout outlasts the setup function's 900 s by a minute, so that a
// FAILED reply arrives in time even when Lambda's queue held the request.
const (
	lookupTimeoutSeconds        = 300
	catalogTimeoutSeconds       = 300
	archiveSchemaTimeoutSeconds = 960
)

const (
	// inArchive is the condition that holds in the regions that archive.
	inArchive = "InArchive"
	// lookupGeneration is the parameter whose new value makes the stream lookups run again.
	lookupGeneration = "LookupGeneration"
)

// regionalTemplate returns regional.yaml: the regional scope, with version as the catalog's
// generator. When only some regions archive, the archive-only resources have the condition
// InArchive, and texts that refer to them are left out elsewhere. The alarms notify the SNS
// topic of the AlarmTopicArn parameter, if it is set.
func regionalTemplate(m *infra.Manifest, version string) (*resources, *yaml.Node, error) {
	s := m.Regional
	r := &resolver{local: map[string]bool{}, global: map[string]infra.Table{}, archiveOnly: map[string]bool{}}
	for _, t := range m.Global.Tables {
		r.global[t.ID] = t
	}
	for _, id := range s.IDs() {
		r.local[id] = true
	}
	for _, id := range s.ArchiveOnlyIDs() {
		r.archiveOnly[id] = true
	}
	rs := newResources(r)
	for _, t := range s.Tables {
		rs.add(t.ID, func() *yaml.Node { return regionalTable(r, t) })
		autoScaling(rs, t)
	}
	for _, b := range s.Buckets {
		rs.add(b.ID, func() *yaml.Node { return bucket(r, b) })
		rs.add(b.ID+"Policy", func() *yaml.Node { return bucketPolicy(b.ID) })
	}
	for _, l := range s.LogGroups {
		rs.add(l.ID, func() *yaml.Node { return logGroup(r, l) })
	}
	for _, l := range s.LogStreams {
		rs.add(l.ID, func() *yaml.Node { return logStream(r, l) })
	}
	for _, x := range s.Roles {
		addRole(rs, r, x)
	}
	for _, f := range s.Functions {
		rs.add(f.ID, func() *yaml.Node { return function(r, f) })
	}
	for _, x := range s.StreamLookups {
		rs.add(x.ID, func() *yaml.Node { return lookup(r, x) })
	}
	for _, x := range s.Mappings {
		rs.add(x.ID, func() *yaml.Node { return eventSourceMapping(r, x) })
	}
	for _, x := range s.StateMachines {
		rs.add(x.ID, func() *yaml.Node { return stateMachine(r, x) })
	}
	addArchive(rs, r, s)
	for _, x := range s.Catalogs {
		rs.add(x.ID, func() *yaml.Node { return catalogRegistration(r, x, version) })
	}
	for _, x := range s.Alarms {
		rs.add(x.ID, func() *yaml.Node { return alarm(r, x) })
	}
	for _, x := range s.Dashboards {
		rs.add(x.ID, func() *yaml.Node { return dashboard(r, x) })
	}
	if rs.err != nil {
		return nil, nil, rs.err
	}
	parameters := mapping(kv("Prefix", prefixParameter(m.Prefix)))
	if len(s.StreamLookups) > 0 {
		parameters.Content = append(parameters.Content, str(lookupGeneration), lookupGenerationParameter())
	}
	var hasTopic *yaml.Node
	if len(s.Alarms) > 0 {
		parameters.Content = append(parameters.Content, str(alarmTopicArn), alarmTopicParameter())
		hasTopic = hasAlarmTopic()
	}
	mappings, archives := regionSets(m)
	conditions := mapping(kv(hasAlarmTopicArn, hasTopic), kv(inArchive, archives))
	if len(conditions.Content) == 0 {
		conditions = nil
	}
	return rs, template("Dynaflux store "+m.Store+": the regional resources. Deploy in every region of the store, after global.yaml.", true,
		kv("Parameters", parameters),
		kv("Mappings", mappings),
		kv("Conditions", conditions),
		kv("Resources", rs.node),
	), nil
}

func lookupGenerationParameter() *yaml.Node {
	return mapping(
		kv("Type", str("String")),
		kv("Default", str("1")),
		kv("Description", str("Change it, to 2 for example, to look up the stream ARNs again after a stream table or replica was re-created.")),
	)
}

// regionSets returns the Mappings section that tells the regions that archive and the
// condition that holds in them, or nothing when every region archives.
func regionSets(m *infra.Manifest) (*yaml.Node, *yaml.Node) {
	if len(m.ArchiveRegions) == 0 {
		return nil, nil
	}
	sets := mapping()
	for _, region := range m.Regions {
		archives := strconv.FormatBool(slices.Contains(m.ArchiveRegions, region))
		sets.Content = append(sets.Content, str(region), mapping(kv("Archive", str(archives))))
	}
	archives := intrinsic("FindInMap", seq(str("RegionSets"), intrinsic("Ref", str("AWS::Region")), str("Archive")))
	return mapping(kv("RegionSets", sets)), intrinsic("Equals", seq(archives, str("true")))
}

func regionalTable(r *resolver, t infra.Table) *yaml.Node {
	var throughput *yaml.Node
	if t.Capacity.Mode == infra.CapacityProvisioned {
		throughput = mapping(kv("ReadCapacityUnits", num(t.Capacity.Read.Min)), kv("WriteCapacityUnits", num(t.Capacity.Write.Min)))
	}
	return resource("AWS::DynamoDB::Table", mapping(
		kv("TableName", r.value(t.Name)),
		kv("AttributeDefinitions", attributeDefinitions(t.Keys)),
		kv("KeySchema", keySchema(t.Keys)),
		kv("BillingMode", str(billingMode(t.Capacity.Mode))),
		kv("ProvisionedThroughput", throughput),
		kv("TimeToLiveSpecification", timeToLive(t.TTLAttribute)),
		kv("DeletionProtectionEnabled", boolean(t.DeletionProtection)),
		kv("PointInTimeRecoverySpecification", mapping(kv("PointInTimeRecoveryEnabled", boolean(t.PointInTimeRecovery)))),
		kv("Tags", tags(t.Tags)),
	), deletion(t.Deletion)...)
}

// autoScaling adds the Application Auto Scaling target and target tracking policy of each
// dimension of a provisioned table. The targets use the service-linked role.
func autoScaling(rs *resources, t infra.Table) {
	if t.Capacity.Mode != infra.CapacityProvisioned {
		return
	}
	for _, d := range []struct {
		name string
		s    *infra.Scaling
	}{{"Read", t.Capacity.Read}, {"Write", t.Capacity.Write}} {
		target := d.name + "Target" + t.ID
		rs.add(target, func() *yaml.Node {
			return resource("AWS::ApplicationAutoScaling::ScalableTarget", mapping(
				kv("ServiceNamespace", str("dynamodb")),
				kv("ResourceId", intrinsic("Sub", str("table/${"+t.ID+"}"))),
				kv("ScalableDimension", str("dynamodb:table:"+d.name+"CapacityUnits")),
				kv("MinCapacity", num(d.s.Min)),
				kv("MaxCapacity", num(d.s.Max)),
			))
		})
		rs.add(d.name+"Policy"+t.ID, func() *yaml.Node {
			return resource("AWS::ApplicationAutoScaling::ScalingPolicy", mapping(
				// A policy's name is unique only within its target.
				kv("PolicyName", str("dynaflux-"+strings.ToLower(d.name))),
				kv("PolicyType", str("TargetTrackingScaling")),
				kv("ScalingTargetId", intrinsic("Ref", str(target))),
				kv("TargetTrackingScalingPolicyConfiguration", mapping(
					kv("TargetValue", num(d.s.TargetPercent)),
					kv("PredefinedMetricSpecification", mapping(
						kv("PredefinedMetricType", str("DynamoDB"+d.name+"CapacityUtilization")),
					)),
				)),
			))
		})
	}
}

func bucket(r *resolver, b infra.Bucket) *yaml.Node {
	enabled := str("Enabled")
	rules := seq(
		mapping(
			kv("Id", str("abort-incomplete-uploads")),
			kv("Status", enabled),
			kv("AbortIncompleteMultipartUpload", mapping(kv("DaysAfterInitiation", num(b.AbortMultipartDays)))),
		),
		// Leftover delete markers would slow every listing.
		mapping(
			kv("Id", str("expire-noncurrent-versions")),
			kv("Status", enabled),
			kv("NoncurrentVersionExpiration", mapping(kv("NoncurrentDays", num(b.NoncurrentExpiryDays)))),
			kv("ExpiredObjectDeleteMarker", boolean(true)),
		),
	)
	for _, e := range b.Expiries {
		rules.Content = append(rules.Content, mapping(
			kv("Id", str(e.ID)),
			kv("Status", enabled),
			kv("Prefix", str(e.Prefix)),
			kv("ExpirationInDays", num(e.Days)),
			kv("NoncurrentVersionExpiration", mapping(kv("NoncurrentDays", num(e.NoncurrentDays)))),
		))
	}
	return resource("AWS::S3::Bucket", mapping(
		kv("BucketName", r.value(b.Name)),
		kv("BucketNamespace", str("account-regional")),
		kv("BucketEncryption", mapping(kv("ServerSideEncryptionConfiguration", seq(
			mapping(kv("ServerSideEncryptionByDefault", mapping(kv("SSEAlgorithm", str("AES256"))))),
		)))),
		kv("OwnershipControls", mapping(kv("Rules", seq(mapping(kv("ObjectOwnership", str("BucketOwnerEnforced"))))))),
		kv("PublicAccessBlockConfiguration", mapping(
			kv("BlockPublicAcls", boolean(true)),
			kv("BlockPublicPolicy", boolean(true)),
			kv("IgnorePublicAcls", boolean(true)),
			kv("RestrictPublicBuckets", boolean(true)),
		)),
		kv("VersioningConfiguration", mapping(kv("Status", enabled))),
		kv("LifecycleConfiguration", mapping(kv("Rules", rules))),
		kv("Tags", tags(b.Tags)),
	), deletion(b.Deletion)...)
}

// bucketPolicy denies every request to the bucket made without TLS.
func bucketPolicy(id string) *yaml.Node {
	return resource("AWS::S3::BucketPolicy", mapping(
		kv("Bucket", intrinsic("Ref", str(id))),
		kv("PolicyDocument", policyDocument(mapping(
			kv("Sid", str("TLSOnly")),
			kv("Effect", str("Deny")),
			kv("Principal", str("*")),
			kv("Action", str("s3:*")),
			kv("Resource", seq(intrinsic("GetAtt", str(id+".Arn")), intrinsic("Sub", str("${"+id+".Arn}/*")))),
			kv("Condition", mapping(kv("Bool", mapping(kv("aws:SecureTransport", str("false")))))),
		))),
	))
}

func logGroup(r *resolver, l infra.LogGroup) *yaml.Node {
	return resource("AWS::Logs::LogGroup", mapping(
		kv("LogGroupName", r.value(l.Name)),
		kv("RetentionInDays", num(l.RetentionDays)),
		kv("Tags", tags(l.Tags)),
	), archiveCondition(l.Common)...)
}

func logStream(r *resolver, l infra.LogStream) *yaml.Node {
	return resource("AWS::Logs::LogStream", mapping(
		kv("LogGroupName", r.value(l.Group)),
		kv("LogStreamName", str(l.Name)),
	), archiveCondition(l.Common)...)
}

// archiveCondition returns the Condition attribute of an archive-only resource.
func archiveCondition(c infra.Common) []pair {
	if !c.ArchiveOnly {
		return nil
	}
	return []pair{kv("Condition", str(inArchive))}
}

// ifArchive returns n in the regions that archive, and nothing elsewhere.
func ifArchive(n *yaml.Node) *yaml.Node {
	return intrinsic("If", seq(str(inArchive), n, intrinsic("Ref", str("AWS::NoValue"))))
}

// addRole adds a role and its managed policies.
func addRole(rs *resources, r *resolver, x infra.Role) {
	rs.add(x.ID, func() *yaml.Node { return role(r, x) })
	for _, p := range x.Managed {
		rs.add(p.ID, func() *yaml.Node { return managedPolicy(r, x, p) })
	}
}

func role(r *resolver, x infra.Role) *yaml.Node {
	statements := seq()
	for _, st := range x.Statements {
		n := statement(r, st)
		if st.ArchiveOnly {
			n = ifArchive(n)
		}
		statements.Content = append(statements.Content, n)
	}
	trust := mapping(
		kv("Effect", str("Allow")),
		kv("Principal", mapping(kv("Service", str(x.Service)))),
		kv("Action", str("sts:AssumeRole")),
		kv("Condition", condition(r, x.Trust)),
	)
	return resource("AWS::IAM::Role", mapping(
		kv("AssumeRolePolicyDocument", policyDocument(trust)),
		kv("Policies", seq(mapping(kv("PolicyName", str("dynaflux")), kv("PolicyDocument", policyDocument(statements.Content...))))),
		kv("Tags", tags(x.Tags)),
	), archiveCondition(x.Common)...)
}

func policyDocument(statements ...*yaml.Node) *yaml.Node {
	return mapping(kv("Version", str("2012-10-17")), kv("Statement", seq(statements...)))
}

func statement(r *resolver, st infra.Statement) *yaml.Node {
	actions := make([]*yaml.Node, 0, len(st.Actions))
	for _, a := range st.Actions {
		actions = append(actions, str(a))
	}
	return mapping(
		kv("Sid", str(st.Sid)),
		kv("Effect", str("Allow")),
		kv("Action", oneOrList(actions)),
		kv("Resource", oneOrList(values(r, st.Resources))),
		kv("Condition", condition(r, st.Conditions)),
	)
}

// condition returns the Condition of a policy statement, or nil when cs is empty, which
// mapping leaves out.
func condition(r *resolver, cs []infra.Condition) *yaml.Node {
	if len(cs) == 0 {
		return nil
	}
	n := mapping()
	for _, c := range cs {
		op := mappingValue(n, c.Operator)
		if op == nil {
			op = mapping()
			n.Content = append(n.Content, str(c.Operator), op)
		}
		op.Content = append(op.Content, str(c.Key), oneOrList(values(r, c.Values)))
	}
	return n
}

func values(r *resolver, ts []infra.Text) []*yaml.Node {
	ns := make([]*yaml.Node, 0, len(ts))
	for _, t := range ts {
		ns = append(ns, r.value(t))
	}
	return ns
}

func oneOrList(ns []*yaml.Node) *yaml.Node {
	if len(ns) == 1 {
		return ns[0]
	}
	return seq(ns...)
}

// mappingValue returns the value of key in mapping n, or nil.
func mappingValue(n *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// function returns a SAM function, which sam build compiles from Code. Every function has
// the same Metadata and nothing else in it: SAM CLI builds functions together only when
// their metadata is equal, so the binary is compiled once.
func function(r *resolver, f infra.Function) *yaml.Node {
	env := mapping()
	for _, k := range slices.Sorted(maps.Keys(f.Env)) {
		env.Content = append(env.Content, str(k), r.optional(f.Env[k]))
	}
	tagMap := mapping()
	for _, k := range slices.Sorted(maps.Keys(f.Tags)) {
		tagMap.Content = append(tagMap.Content, str(k), str(f.Tags[k]))
	}
	return resource("AWS::Serverless::Function", mapping(
		kv("FunctionName", r.value(f.Name)),
		kv("CodeUri", str(f.Code)),
		kv("Runtime", str(f.Runtime)),
		kv("Handler", str(f.Handler)),
		kv("Architectures", seq(str(f.Arch))),
		kv("MemorySize", num(f.MemoryMB)),
		kv("Timeout", num(f.TimeoutSeconds)),
		kv("Role", r.value(f.Role)),
		kv("LoggingConfig", mapping(kv("LogGroup", r.value(f.LogGroup)), kv("LogFormat", str("JSON")))),
		kv("Environment", mapping(kv("Variables", env))),
		kv("Tags", tagMap),
	), kv("Metadata", mapping(kv("BuildMethod", str("go1.x")))))
}

// lookup returns the custom resource that the setup function answers with the stream ARN
// of the stream table's replica in the stack's region (ADR 0008). CloudFormation sends it
// again only when its properties change, which a new LookupGeneration does.
func lookup(r *resolver, x infra.StreamLookup) *yaml.Node {
	return resource(setup.TypeStreamARN, mapping(
		kv("ServiceToken", r.value(x.Handler)),
		kv("ServiceTimeout", num(lookupTimeoutSeconds)),
		kv("TableName", r.value(x.Table)),
		kv(lookupGeneration, intrinsic("Ref", str(lookupGeneration))),
	))
}

// catalogRegistration returns the custom resource that the setup function answers by
// registering the binary's catalog (ADR 0013). A new CatalogHash or Generator sends Update.
func catalogRegistration(r *resolver, x infra.CatalogRegistration, generator string) *yaml.Node {
	return resource(setup.TypeCatalog, mapping(
		kv("ServiceToken", r.value(x.Handler)),
		kv("ServiceTimeout", num(catalogTimeoutSeconds)),
		kv("MetaTable", r.value(x.MetaTable)),
		kv("CatalogHash", str(x.Hash)),
		kv("Generator", str(generator)),
	), dependsOn(x.After...))
}

func eventSourceMapping(r *resolver, x infra.Mapping) *yaml.Node {
	metrics := seq()
	for _, m := range x.Metrics {
		if m != infra.MetricEventCount {
			r.fail(fmt.Errorf("unknown metric %s", m))
		}
		metrics.Content = append(metrics.Content, str("EventCount"))
	}
	var responseTypes *yaml.Node
	if x.ReportBatchItemFailures {
		responseTypes = seq(str("ReportBatchItemFailures"))
	}
	return resource("AWS::Lambda::EventSourceMapping", mapping(
		kv("FunctionName", r.value(x.Function)),
		kv("EventSourceArn", r.value(x.Source)),
		kv("StartingPosition", str(strings.ToUpper(x.StartingPosition))),
		kv("BatchSize", num(x.BatchSize)),
		kv("MaximumBatchingWindowInSeconds", num(x.BatchingWindowSeconds)),
		kv("ParallelizationFactor", num(x.ParallelizationFactor)),
		kv("BisectBatchOnFunctionError", boolean(x.BisectOnError)),
		kv("FunctionResponseTypes", responseTypes),
		kv("MaximumRetryAttempts", num(x.MaxRetryAttempts)),
		kv("MaximumRecordAgeInSeconds", num(x.MaxRecordAgeSeconds)),
		kv("TumblingWindowInSeconds", num(x.TumblingWindowSeconds)),
		kv("DestinationConfig", mapping(kv("OnFailure", mapping(kv("Destination", r.value(x.OnFailure)))))),
		kv("MetricsConfig", mapping(kv("Metrics", metrics))),
	))
}
