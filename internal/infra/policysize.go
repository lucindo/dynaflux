// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"encoding/json"
	"fmt"
	"strings"
)

// maxRolePolicySize is IAM's limit on the inline policies of one role, in characters
// without whitespace.
const maxRolePolicySize = 10240

// The longest values that a deployed policy can hold in place of a substitution.
const (
	longestPartition = "aws-us-gov"
	roleNameLen      = 64 // IAM's limit; CloudFormation names the roles
	uuidLen          = 36
)

// policyDocument is an IAM policy as IAM measures it: JSON without whitespace.
type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Sid       string                         `json:"Sid"`
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// checkPolicySizes fails when a role's policies are over IAM's limit, resolved by w with the
// longest prefix that the names allow. The fix it suggests depends on whether each stream
// has its own projector.
func (m *Manifest) checkPolicySizes(w worstCase, projectorPerStream bool) error {
	fix := "set runtime.split: stream, which gives each stream's projector a role of its own, or retire old view versions"
	if projectorPerStream {
		fix = "retire old view versions"
	}
	w.prefixLen, w.byID = m.Prefix.MaxLen, map[string]entry{}
	for _, e := range m.entries() {
		w.byID[e.id] = e
	}
	for _, s := range []Scope{m.Global, m.Regional} {
		for _, r := range s.Roles {
			size, err := w.policySize(r)
			if err != nil {
				return fmt.Errorf("infra: %s: %w", r.ID, err)
			}
			if size > maxRolePolicySize {
				return fmt.Errorf("infra: the policies of role %s can reach %d characters, over IAM's limit of %d per role; %s",
					r.ID, size, maxRolePolicySize, fix)
			}
		}
	}
	return nil
}

// worstCase resolves texts to the longest values they can take when deployed: the longest
// region of the store, a 12-digit account ID, the longest partition and a prefix of
// prefixLen characters.
type worstCase struct {
	region    string
	prefixLen int
	// byID holds the resources that references name; texts without references need none.
	byID map[string]entry
}

func (w worstCase) policySize(r Role) (int, error) {
	doc := policyDocument{Version: "2012-10-17"}
	for _, st := range r.Statements {
		ps := policyStatement{Sid: st.Sid, Effect: "Allow", Action: st.Actions}
		for _, res := range st.Resources {
			v, err := w.text(res)
			if err != nil {
				return 0, err
			}
			ps.Resource = append(ps.Resource, v)
		}
		for _, c := range st.Conditions {
			if err := w.addCondition(&ps, c); err != nil {
				return 0, err
			}
		}
		doc.Statement = append(doc.Statement, ps)
	}
	data, err := json.Marshal(doc)
	return len(data), err
}

func (w worstCase) addCondition(ps *policyStatement, c Condition) error {
	if ps.Condition == nil {
		ps.Condition = map[string]map[string][]string{}
	}
	if ps.Condition[c.Operator] == nil {
		ps.Condition[c.Operator] = map[string][]string{}
	}
	for _, t := range c.Values {
		v, err := w.text(t)
		if err != nil {
			return err
		}
		ps.Condition[c.Operator][c.Key] = append(ps.Condition[c.Operator][c.Key], v)
	}
	return nil
}

func (w worstCase) text(t Text) (string, error) {
	segs, err := t.Segments()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, s := range segs {
		switch {
		case s.Literal != "":
			b.WriteString(s.Literal)
		case s.Placeholder != "":
			b.WriteString(w.placeholder(s.Placeholder))
		default:
			v, refErr := w.ref(s.Ref)
			if refErr != nil {
				return "", refErr
			}
			b.WriteString(v)
		}
	}
	return b.String(), nil
}

func (w worstCase) placeholder(p string) string {
	switch p {
	case PlaceholderRegion:
		return w.region
	case PlaceholderAccount:
		return strings.Repeat("0", accountIDLen)
	case PlaceholderPartition:
		return longestPartition
	default: // the prefix, with or without "_"
		return strings.Repeat("p", w.prefixLen)
	}
}

// ref returns the value of a reference, by the ARN format of the referenced resource's
// kind.
func (w worstCase) ref(r Ref) (string, error) {
	e := w.byID[r.ID]
	name, err := w.text(e.name)
	if err != nil {
		return "", err
	}
	account := w.placeholder(PlaceholderAccount)
	regional := func(service, resource string) string {
		return "arn:" + longestPartition + ":" + service + ":" + w.region + ":" + account + ":" + resource
	}
	table := regional("dynamodb", "table/"+name)
	switch r.Attr {
	case AttrName:
		return name, nil
	case AttrUUID:
		return strings.Repeat("0", uuidLen), nil
	case AttrStreamARNPattern:
		return table + "/stream/*", nil
	}
	switch e.kind {
	case kindTable:
		return table, nil
	case kindBucket:
		return "arn:" + longestPartition + ":s3:::" + name, nil
	case kindLogGroup:
		return regional("logs", "log-group:"+name+":*"), nil
	case kindRole:
		return "arn:" + longestPartition + ":iam::" + account + ":role/" + strings.Repeat("r", roleNameLen), nil
	case kindFunction:
		return regional("lambda", "function:"+name), nil
	case kindDeliveryStream:
		return regional("firehose", "deliverystream/"+name), nil
	}
	return "", fmt.Errorf("no ARN format for a %s", e.kind)
}
