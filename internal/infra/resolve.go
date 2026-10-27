// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"fmt"
	"slices"
	"strings"
)

// Deployment is one deployment of a manifest: the values of its placeholders.
type Deployment struct {
	Prefix, Partition, Account, Region string
}

// Resolve returns t as deployment d reads it, references included, as IAM sees a policy
// statement. It resolves references to tables, buckets, delivery streams, archive workgroups
// and roles only, and fails on any other. CloudFormation names a role when it creates it, so
// a role's ID stands in for its name.
func (m *Manifest) Resolve(t Text, d Deployment) (string, error) {
	segs, err := t.Segments()
	if err != nil {
		return "", fmt.Errorf("infra: %w", err)
	}
	var b strings.Builder
	for _, s := range segs {
		switch {
		case s.Placeholder != "":
			b.WriteString(d.value(s.Placeholder))
		case s.Ref != Ref{}:
			v, rerr := m.resolveRef(s.Ref, d)
			if rerr != nil {
				return "", rerr
			}
			b.WriteString(v)
		default:
			b.WriteString(s.Literal)
		}
	}
	return b.String(), nil
}

func (d Deployment) value(placeholder string) string {
	switch placeholder {
	case PlaceholderPrefix:
		return d.Prefix
	case PlaceholderPrefixUnderscore:
		return strings.ReplaceAll(d.Prefix, "-", "_")
	case PlaceholderRegion:
		return d.Region
	case PlaceholderAccount:
		return d.Account
	}
	return d.Partition // Segments accepts no other placeholder
}

func (m *Manifest) resolveRef(ref Ref, d Deployment) (string, error) {
	var name Text
	var service, resource string
	for _, s := range []*Scope{&m.Global, &m.Regional} {
		if i := slices.IndexFunc(s.Tables, func(t Table) bool { return t.ID == ref.ID }); i >= 0 {
			name, service, resource = s.Tables[i].Name, "dynamodb", "table/"
		}
		if i := slices.IndexFunc(s.Buckets, func(x Bucket) bool { return x.ID == ref.ID }); i >= 0 {
			name, service = s.Buckets[i].Name, "s3"
		}
		if i := slices.IndexFunc(s.DeliveryStreams, func(ds DeliveryStream) bool { return ds.ID == ref.ID }); i >= 0 {
			name, service, resource = s.DeliveryStreams[i].Name, "firehose", "deliverystream/"
		}
		if i := slices.IndexFunc(s.ArchiveWorkGroups, func(w ArchiveWorkGroup) bool { return w.ID == ref.ID }); i >= 0 {
			name, service, resource = s.ArchiveWorkGroups[i].Name, "athena", "workgroup/"
		}
		if slices.ContainsFunc(s.Roles, func(r Role) bool { return r.ID == ref.ID }) {
			name, service, resource = Text(ref.ID), "iam", "role/"
		}
	}
	if service == "" {
		return "", fmt.Errorf("infra: ${ref:%s.%s}: Resolve resolves references to tables, buckets, delivery streams, "+
			"archive workgroups and roles only", ref.ID, ref.Attr)
	}
	n, err := m.Resolve(name, d)
	if err != nil {
		return "", err
	}
	region, account := d.Region, d.Account
	switch service {
	case "iam":
		region = "" // IAM is global
	case "s3":
		region, account = "", "" // bucket ARNs hold neither
	}
	arn := "arn:" + d.Partition + ":" + service + ":" + region + ":" + account + ":" + resource + n
	switch {
	case ref.Attr == AttrName:
		return n, nil
	case ref.Attr == AttrARN:
		return arn, nil
	case ref.Attr == AttrStreamARNPattern && service == "dynamodb":
		return arn + "/stream/*", nil
	}
	return "", fmt.Errorf("infra: ${ref:%s.%s}: a %s resource doesn't provide %s", ref.ID, ref.Attr, service, ref.Attr)
}
