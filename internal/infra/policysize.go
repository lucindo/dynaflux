// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// IAM's limits, in characters without whitespace for sizes: the inline policies of one role,
// one managed policy, and the managed policies attached to a role by default.
const (
	maxRolePolicySize    = 10240
	maxManagedPolicySize = 6144
	maxManagedPolicies   = 10
)

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
	w = m.resolving(w)
	for _, s := range m.scopes() {
		for _, r := range s.Roles {
			size, err := w.policySize(r.Statements)
			if err != nil {
				return fmt.Errorf("infra: %s: %w", r.ID, err)
			}
			if size > maxRolePolicySize {
				return fmt.Errorf("infra: the policies of role %s can reach %d characters, over IAM's limit of %d per role; %s",
					r.ID, size, maxRolePolicySize, fix)
			}
			if err = w.checkManaged(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolving returns w with the longest prefix that m's names allow and m's resources, which
// references name.
func (m *Manifest) resolving(w worstCase) worstCase {
	w.prefixLen, w.byID = m.Prefix.MaxLen, map[string]entry{}
	for _, e := range m.entries() {
		w.byID[e.id] = e
	}
	return w
}

// checkManaged fails when a managed policy of r is over IAM's limit, or r has more than IAM
// attaches to a role by default.
func (w worstCase) checkManaged(r Role) error {
	if len(r.Managed) > maxManagedPolicies {
		return fmt.Errorf("infra: role %s needs %d managed policies, and IAM attaches at most %d to a role by default; "+
			"retire old view versions, or split the streams across stores", r.ID, len(r.Managed), maxManagedPolicies)
	}
	for _, p := range r.Managed {
		size, err := w.policySize(p.Statements)
		if err != nil {
			return fmt.Errorf("infra: %s: %w", p.ID, err)
		}
		if size > maxManagedPolicySize {
			return fmt.Errorf("infra: managed policy %s can reach %d characters, over IAM's limit of %d", p.ID, size, maxManagedPolicySize)
		}
	}
	return nil
}

// packManaged splits the managed statements of each role of roles into as few policies as
// fit IAM's limit on one, each statement in order and its resources in order, and numbers
// them. A resource that alone overflows a policy gets one, which checkManaged then reports.
func (w worstCase) packManaged(roles []Role) error {
	for i := range roles {
		r := &roles[i]
		if len(r.Managed) == 0 {
			continue
		}
		var all []Statement
		for _, p := range r.Managed {
			all = append(all, p.Statements...)
		}
		packed, err := w.pack(all)
		if err != nil {
			return fmt.Errorf("infra: %s: %w", r.ID, err)
		}
		r.Managed = r.Managed[:0]
		for n, sts := range packed {
			r.Managed = append(r.Managed, ManagedPolicy{ID: r.ID + "Policy" + strconv.Itoa(n+1), Statements: sts})
		}
	}
	return nil
}

// pack splits statements into policies of at most maxManagedPolicySize characters.
func (w worstCase) pack(statements []Statement) ([][]Statement, error) {
	var packed [][]Statement
	var cur []Statement
	for _, st := range statements {
		for _, res := range st.Resources {
			next := withResource(cur, st, res)
			size, err := w.policySize(next)
			if err != nil {
				return nil, err
			}
			if size > maxManagedPolicySize && len(cur) > 0 {
				packed = append(packed, cur)
				next = withResource(nil, st, res)
			}
			cur = next
		}
	}
	return append(packed, cur), nil
}

// withResource returns sts with res added to statement st: to its last statement when that
// is st's, or else to a new copy of st.
func withResource(sts []Statement, st Statement, res Text) []Statement {
	out := slices.Clone(sts)
	if n := len(out); n > 0 && out[n-1].Sid == st.Sid {
		out[n-1].Resources = append(slices.Clone(out[n-1].Resources), res)
		return out
	}
	st.Resources = []Text{res}
	return append(out, st)
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

func (w worstCase) policySize(statements []Statement) (int, error) {
	doc := policyDocument{Version: "2012-10-17"}
	for _, st := range statements {
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
