// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// FunctionRole returns the role of the regional function that runs in mode.
func (m *Manifest) FunctionRole(mode string) (Role, error) {
	i := slices.IndexFunc(m.Regional.Functions, func(f Function) bool { return f.Mode == mode })
	if i < 0 {
		return Role{}, fmt.Errorf("infra: no function runs in mode %s", mode)
	}
	role := m.Regional.Functions[i].Role
	segs, err := role.Segments()
	if err != nil {
		return Role{}, fmt.Errorf("infra: %w", err)
	}
	if len(segs) != 1 || segs[0].Ref == (Ref{}) {
		return Role{}, fmt.Errorf("infra: the %s function's role %q isn't one reference", mode, role)
	}
	j := slices.IndexFunc(m.Regional.Roles, func(r Role) bool { return r.ID == segs[0].Ref.ID })
	if j < 0 {
		return Role{}, fmt.Errorf("infra: no role %s", segs[0].Ref.ID)
	}
	return m.Regional.Roles[j], nil
}

// Allowing returns the Sids of r's statements that allow action on arn in deployment d, for
// a request with the condition keys keys. In a resource, * matches any run of characters
// and ? any one. It evaluates StringEquals conditions only, and fails on any other that it
// would need.
func (m *Manifest) Allowing(r Role, d Deployment, action, arn string, keys map[string]string) ([]string, error) {
	var sids []string
	for _, st := range r.Statements {
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
		p = strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(p))
		if regexp.MustCompile("^" + p + "$").MatchString(arn) {
			return true, nil
		}
	}
	return false, nil
}

// holds reports whether every condition of st holds for a request with keys.
func (m *Manifest) holds(st Statement, d Deployment, keys map[string]string) (bool, error) {
	for _, c := range st.Conditions {
		if c.Operator != condStringEquals {
			return false, fmt.Errorf("infra: statement %s: condition operator %s isn't supported", st.Sid, c.Operator)
		}
		v, ok := keys[c.Key]
		if !ok {
			return false, nil
		}
		var equal bool
		for _, want := range c.Values {
			w, err := m.Resolve(want, d)
			if err != nil {
				return false, err
			}
			equal = equal || w == v
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}
