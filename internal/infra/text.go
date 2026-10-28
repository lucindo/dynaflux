// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"fmt"
	"strings"
)

// Text is a string that may hold substitutions, which a renderer resolves:
//
//	${prefix}         the deployment prefix, at most Prefix.MaxLen characters
//	${prefix_}        the prefix with every "-" replaced by "_", for Glue names
//	${region}         the region the scope is deployed in
//	${account}        the AWS account ID
//	${partition}      the AWS partition, such as aws
//	${url_suffix}     the domain of the partition's endpoints, such as amazonaws.com
//	${domain}         the custom domain of the query scope, chosen at deploy time
//	${hosted_zone}    the ID of the Route 53 hosted zone of ${domain}, chosen at deploy time
//	${ref:ID.ATTR}    attribute ATTR of the resource with ID
//
// References are the dependency graph: a renderer creates or updates a referenced
// resource before the one that refers to it, so it must express each reference in its
// tool's dependency form, never as a copied literal.
type Text string

// Placeholders of a [Text], besides references.
const (
	PlaceholderPrefix           = "prefix"
	PlaceholderPrefixUnderscore = "prefix_"
	PlaceholderRegion           = "region"
	PlaceholderAccount          = "account"
	PlaceholderPartition        = "partition"
	PlaceholderURLSuffix        = "url_suffix"
	PlaceholderDomain           = "domain"
	PlaceholderHostedZone       = "hosted_zone"
)

// Ref is a reference to attribute Attr of the resource with ID.
type Ref struct {
	ID   string
	Attr string
}

// Attributes that resources provide to references. Every kind with a name provides
// AttrName, and every kind with an ARN AttrARN. A stream table provides AttrStreamARNPattern,
// which matches its streams in IAM policies. A stream lookup provides only AttrStreamARN, a
// mapping only AttrUUID, an archive schema only AttrFingerprint, and a function URL only
// AttrURL. The resources of API Gateway and Route 53 that AWS identifies by an ID provide
// AttrID, and a custom domain AttrRegionalName and AttrRegionalZone.
const (
	AttrName             = "name"
	AttrARN              = "arn"
	AttrStreamARNPattern = "stream_arn_pattern"
	AttrStreamARN        = "stream_arn"
	AttrUUID             = "uuid"
	AttrFingerprint      = "fingerprint"
	AttrURL              = "url"
	AttrID               = "id"
	AttrRegionalName     = "regional_name"
	AttrRegionalZone     = "regional_zone"
)

// Segment is a part of a [Text]: literal text, a placeholder or a reference. Exactly one
// field is set.
type Segment struct {
	Literal     string
	Placeholder string
	Ref         Ref
}

// Segments splits t into its parts. It fails on a substitution that [Text] doesn't list,
// and on a "${" without its "}".
func (t Text) Segments() ([]Segment, error) {
	var segs []Segment
	rest := string(t)
	for rest != "" {
		lit, sub, found := strings.Cut(rest, "${")
		if lit != "" {
			segs = append(segs, Segment{Literal: lit})
		}
		if !found {
			break
		}
		inner, after, closed := strings.Cut(sub, "}")
		if !closed {
			return nil, fmt.Errorf("%q has a ${ without its }", t)
		}
		seg, err := segment(inner)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", t, err)
		}
		segs = append(segs, seg)
		rest = after
	}
	return segs, nil
}

func segment(inner string) (Segment, error) {
	switch inner {
	case PlaceholderPrefix, PlaceholderPrefixUnderscore, PlaceholderRegion, PlaceholderAccount, PlaceholderPartition,
		PlaceholderURLSuffix, PlaceholderDomain, PlaceholderHostedZone:
		return Segment{Placeholder: inner}, nil
	}
	target, ok := strings.CutPrefix(inner, "ref:")
	if !ok {
		return Segment{}, fmt.Errorf("unknown substitution ${%s}", inner)
	}
	id, attr, ok := strings.Cut(target, ".")
	if !ok || id == "" || attr == "" {
		return Segment{}, fmt.Errorf("${%s} isn't a reference of the form ${ref:ID.ATTR}", inner)
	}
	return Segment{Ref: Ref{ID: id, Attr: attr}}, nil
}

func placeholder(name string) string {
	return "${" + name + "}"
}

func ref(id, attr string) Text {
	return Text("${ref:" + id + "." + attr + "}")
}
