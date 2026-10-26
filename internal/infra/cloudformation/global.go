// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"regexp"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/infra"
)

// prefixPattern returns the pattern that accepts an empty prefix or the form of prefix in
// dynaflux.yaml, and refuses a prefix that S3 reserves at the start of bucket names.
func prefixPattern() string {
	reserved := catalog.ReservedBucketPrefixes()
	for i, p := range reserved {
		reserved[i] = regexp.QuoteMeta(p)
	}
	return `^(?!` + strings.Join(reserved, "|") + `)([a-z][a-z0-9-]*-)?$`
}

// globalTemplate returns global.yaml: one Global Table per stream table of the global scope.
func globalTemplate(m *infra.Manifest) (*resources, *yaml.Node, error) {
	r := &resolver{local: map[string]bool{}}
	for _, id := range m.Global.IDs() {
		r.local[id] = true
	}
	rs := newResources(r)
	for _, t := range m.Global.Tables {
		rs.add(t.ID, func() *yaml.Node { return globalTable(r, t) })
	}
	if rs.err != nil {
		return nil, nil, rs.err
	}
	return rs, template("Dynaflux store "+m.Store+": the stream tables. Deploy once, in the store's home region.", false,
		kv("Parameters", mapping(kv("Prefix", prefixParameter(m.Prefix)))), kv("Resources", rs.node)), nil
}

func prefixParameter(p infra.Prefix) *yaml.Node {
	return mapping(
		kv("Type", str("String")),
		kv("Default", str(p.Default)),
		kv("AllowedPattern", str(prefixPattern())),
		kv("MaxLength", num(p.MaxLen)),
		kv("Description", str("Prefix of every resource name. Fixed for the life of the stack: a new prefix means new stacks.")),
	)
}

func globalTable(r *resolver, t infra.Table) *yaml.Node {
	replicas := seq()
	for _, region := range t.Replicas {
		replicas.Content = append(replicas.Content, mapping(
			kv("Region", str(region)),
			kv("DeletionProtectionEnabled", boolean(t.DeletionProtection && region != t.UnprotectedRegion)),
			kv("PointInTimeRecoverySpecification", mapping(kv("PointInTimeRecoveryEnabled", boolean(t.PointInTimeRecovery)))),
			kv("ReadProvisionedThroughputSettings", scalingSettings("ReadCapacityAutoScalingSettings", t.Capacity.Read)),
			kv("Tags", tags(t.Tags)),
		))
	}
	// The manifest spells consistency, stream view and key roles as CloudFormation does,
	// in lower case.
	return resource("AWS::DynamoDB::GlobalTable", mapping(
		kv("TableName", r.value(t.Name)),
		kv("AttributeDefinitions", attributeDefinitions(t.Keys)),
		kv("KeySchema", keySchema(t.Keys)),
		kv("BillingMode", str(billingMode(t.Capacity.Mode))),
		kv("WriteProvisionedThroughputSettings", scalingSettings("WriteCapacityAutoScalingSettings", t.Capacity.Write)),
		kv("MultiRegionConsistency", str(strings.ToUpper(t.Consistency))),
		kv("StreamSpecification", mapping(kv("StreamViewType", str(strings.ToUpper(t.StreamView))))),
		kv("TimeToLiveSpecification", timeToLive(t.TTLAttribute)),
		kv("Replicas", replicas),
	), deletion(t.Deletion)...)
}

func attributeDefinitions(keys []infra.Key) *yaml.Node {
	defs := seq()
	for _, k := range keys {
		defs.Content = append(defs.Content, mapping(kv("AttributeName", str(k.Name)), kv("AttributeType", str(k.Type))))
	}
	return defs
}

func keySchema(keys []infra.Key) *yaml.Node {
	schema := seq()
	for _, k := range keys {
		schema.Content = append(schema.Content, mapping(kv("AttributeName", str(k.Name)), kv("KeyType", str(strings.ToUpper(string(k.Key))))))
	}
	return schema
}

func timeToLive(attr string) *yaml.Node {
	return mapping(kv("AttributeName", str(attr)), kv("Enabled", boolean(true)))
}

// scalingSettings returns a Global Table's auto scaling settings under key, or nil for a
// table on demand. SeedCapacity is required to switch a table to provisioned.
func scalingSettings(key string, s *infra.Scaling) *yaml.Node {
	if s == nil {
		return nil
	}
	return mapping(kv(key, mapping(
		kv("MinCapacity", num(s.Min)),
		kv("MaxCapacity", num(s.Max)),
		kv("SeedCapacity", num(s.Min)),
		kv("TargetTrackingScalingPolicyConfiguration", mapping(kv("TargetValue", num(s.TargetPercent)))),
	)))
}

func billingMode(m infra.CapacityMode) string {
	if m == infra.CapacityProvisioned {
		return "PROVISIONED"
	}
	return "PAY_PER_REQUEST"
}
