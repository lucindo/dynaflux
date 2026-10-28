// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// FunctionRole returns the role of the regional or query function that runs in mode.
func (m *Manifest) FunctionRole(mode string) (Role, error) {
	for _, s := range []*Scope{&m.Regional, &m.Query} {
		i := slices.IndexFunc(s.Functions, func(f Function) bool { return f.Mode == mode })
		if i < 0 {
			continue
		}
		role := s.Functions[i].Role
		segs, err := role.Segments()
		if err != nil {
			return Role{}, fmt.Errorf("infra: %w", err)
		}
		if len(segs) != 1 || segs[0].Ref == (Ref{}) {
			return Role{}, fmt.Errorf("infra: the %s function's role %q isn't one reference", mode, role)
		}
		j := slices.IndexFunc(s.Roles, func(r Role) bool { return r.ID == segs[0].Ref.ID })
		if j < 0 {
			return Role{}, fmt.Errorf("infra: no role %s", segs[0].Ref.ID)
		}
		return s.Roles[j], nil
	}
	return Role{}, fmt.Errorf("infra: no function runs in mode %s", mode)
}

// Allowing returns the Sids of the statements of r and its managed policies that allow
// action on arn in deployment d, for a request with the condition keys keys. In a resource
// or a StringLike value, * matches any run of characters and ? any one. It evaluates
// StringEquals, StringLike and ForAllValues:StringEquals conditions only, and fails on any
// other that it would need. A key has one value, and as in IAM, ForAllValues holds for a
// request without the key.
func (m *Manifest) Allowing(r Role, d Deployment, action, arn string, keys map[string]string) ([]string, error) {
	statements := slices.Clone(r.Statements)
	for _, p := range r.Managed {
		statements = append(statements, p.Statements...)
	}
	var sids []string
	for _, st := range statements {
		if !slices.Contains(st.Actions, action) {
			continue
		}
		matched, err := m.matchesAny(st.Resources, d, arn)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		holds, err := m.holds(st, d, keys)
		if err != nil {
			return nil, err
		}
		if holds {
			sids = append(sids, st.Sid)
		}
	}
	return sids, nil
}

// matchesAny reports whether one of resources matches arn.
func (m *Manifest) matchesAny(resources []Text, d Deployment, arn string) (bool, error) {
	for _, res := range resources {
		p, err := m.Resolve(res, d)
		if err != nil {
			return false, err
		}
		if glob(p).MatchString(arn) {
			return true, nil
		}
	}
	return false, nil
}

// holds reports whether every condition of st holds for a request with keys.
func (m *Manifest) holds(st Statement, d Deployment, keys map[string]string) (bool, error) {
	for _, c := range st.Conditions {
		if c.Operator != condStringEquals && c.Operator != condStringLike && c.Operator != condForAllValuesStringEquals {
			return false, fmt.Errorf("infra: statement %s: condition operator %s isn't supported", st.Sid, c.Operator)
		}
		v, ok := keys[c.Key]
		if !ok && c.Operator == condForAllValuesStringEquals {
			continue
		}
		if !ok {
			return false, nil
		}
		var matched bool
		for _, want := range c.Values {
			w, err := m.Resolve(want, d)
			if err != nil {
				return false, err
			}
			matched = matched || w == v || c.Operator == condStringLike && glob(w).MatchString(v)
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

// glob returns the expression of an IAM pattern, in which * matches any run of characters and
// ? any one.
func glob(p string) *regexp.Regexp {
	return regexp.MustCompile("^" + strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(p)) + "$")
}
