// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package runtime_test

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/infra"
)

// account is the account ID in DynamoDB Local's ARNs, in which the suite resolves the
// projector role.
const account = "000000000000"

// runTests runs the tests, then checks every call of their projector functions against the
// projector role of the suite's templates: a call that no statement allows would be
// AccessDenied on AWS, and a statement that allows the same call for another deployment or
// store, or on a stream table, grants too much. After a run of the whole suite, it also
// checks that a call used each action the role grants, except where the role acts for
// others, and that controls that break the role fail the check.
func runTests(m *testing.M) int {
	s, err := loadSuite(simStore)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rule suite:", err)
		return 1
	}
	rules = s
	code := m.Run()
	whole := code == 0 && !slices.ContainsFunc([]string{"test.run", "test.skip", "test.list"}, func(name string) bool {
		return flag.Lookup(name).Value.String() != ""
	})
	if err = s.checkGrants(whole); err != nil {
		fmt.Fprintln(os.Stderr, "projector grants:", err)
		return 1
	}
	return code
}

// forOthers reports whether a grant of the projector role, its Sid and action, serves what
// others do as the role, which no call of the suite does: the mapping reads the stream, and
// Lambda writes logs and the batches the mapping discards.
func forOthers(grant string) bool {
	switch grant {
	case "Logs logs:CreateLogStream", "Logs logs:PutLogEvents",
		"ReadStreams dynamodb:DescribeStream", "ReadStreams dynamodb:GetRecords", "ReadStreams dynamodb:GetShardIterator",
		"ListStreams dynamodb:ListStreams", "FailureList s3:ListBucket", "FailurePut s3:PutObject":
		return true
	}
	return false
}

func (s *suite) checkGrants(whole bool) error {
	role, err := s.manifest.FunctionRole(catalog.EnvModeProjector)
	if err != nil {
		return err
	}
	if err = s.allows(role, whole); err != nil || !whole {
		return err
	}
	store := s.cat.Catalog().Store
	tables := "arn:aws:dynamodb:" + emulatortest.Region + ":" + account + ":table/"
	views := "no statement allows dynamodb:PutItem on " + tables + "${prefix}" + store + "-view-"
	widen := func(resource infra.Text) func(st *infra.Statement, _ infra.Role) {
		return func(st *infra.Statement, _ infra.Role) { st.Resources = append(slices.Clone(st.Resources), resource) }
	}
	controls := []struct {
		name, sid string
		change    func(st *infra.Statement, role infra.Role)
		want      string // in the check's error
	}{
		{"an action no call uses", "Meta", func(st *infra.Statement, _ infra.Role) {
			st.Actions = append(slices.Clone(st.Actions), "dynamodb:DeleteItem")
		}, "statement Meta allows dynamodb:DeleteItem, which no call used"},
		{"no action the projector calls", "Views", func(st *infra.Statement, _ infra.Role) {
			st.Actions = slices.DeleteFunc(slices.Clone(st.Actions), func(action string) bool { return action == "dynamodb:PutItem" })
		}, views},
		{"another table", "Views", func(st *infra.Statement, role infra.Role) {
			st.Resources = role.Statements[slices.IndexFunc(role.Statements, func(m infra.Statement) bool { return m.Sid == "Meta" })].Resources
		}, views},
		{"an action others don't use", "FailurePut", func(st *infra.Statement, _ infra.Role) {
			st.Actions = append(slices.Clone(st.Actions), "s3:DeleteObject")
		}, "statement FailurePut allows s3:DeleteObject, which no call used"},
		{"every table", "Views", func(st *infra.Statement, _ infra.Role) { st.Resources = []infra.Text{"*"} },
			"statement Views allows dynamodb:PutItem on " + tables + "dev-" + store + "-meta, another deployment's"},
		{"another prefix's meta table", "Meta", func(st *infra.Statement, _ infra.Role) {
			st.Resources = []infra.Text{infra.Text("arn:${partition}:dynamodb:${region}:${account}:table/*" + store + "-meta")}
		}, "statement Meta allows dynamodb:PutItem on " + tables + "dev-" + store + "-meta, another deployment's"},
		{"another store's views", "Views", widen("arn:${partition}:dynamodb:${region}:${account}:table/${prefix}*-view-*"),
			"statement Views allows dynamodb:PutItem on " + tables + "${prefix}billing-view-"},
		{"the stream tables", "Views", widen(infra.Text("arn:${partition}:dynamodb:${region}:${account}:table/${prefix}" + store + "-*")),
			"statement Views allows dynamodb:PutItem on " + tables + "${prefix}" + store + "-stream-"},
	}
	for _, c := range controls {
		broken := role
		broken.Statements = slices.Clone(role.Statements)
		k := slices.IndexFunc(broken.Statements, func(st infra.Statement) bool { return st.Sid == c.sid })
		if k < 0 {
			return fmt.Errorf("control %q: the role has no statement %s", c.name, c.sid)
		}
		c.change(&broken.Statements[k], role)
		err = s.allows(broken, true)
		if err == nil {
			return fmt.Errorf("control %q: statement %s with it passes the check", c.name, c.sid)
		}
		if !strings.Contains(err.Error(), c.want) {
			return fmt.Errorf("control %q: statement %s with it fails the check with %w, not with %q", c.name, c.sid, err, c.want)
		}
	}
	return nil
}

// allows checks that a statement of role allows each call the suite recorded, and none of
// what the call would name elsewhere; when whole, it also checks that a call used each
// action of every statement, except the grants that serve others.
func (s *suite) allows(role infra.Role, whole bool) error {
	s.mu.Lock()
	runs := slices.Clone(s.calls)
	s.mu.Unlock()
	cat := s.cat.Catalog()
	used := map[string]bool{}    // a statement's Sid and action
	denied := map[string]bool{}  // an action and ARN, with ${prefix} for the test's prefix
	foreign := map[string]bool{} // a statement, an action and an ARN it must not allow, and why
	for _, run := range runs {
		g := grants{
			m: s.manifest, role: role, cat: cat, n: catalog.Naming{Prefix: run.prefix, Store: cat.Store},
			d: infra.Deployment{Prefix: run.prefix, Partition: "aws", Account: account, Region: emulatortest.Region},
		}
		if err := g.check(run.calls, used, denied, foreign); err != nil {
			return err
		}
	}
	var errs []error
	for _, d := range slices.Sorted(maps.Keys(denied)) {
		errs = append(errs, fmt.Errorf("no statement allows %s", d))
	}
	for _, f := range slices.Sorted(maps.Keys(foreign)) {
		errs = append(errs, errors.New(f))
	}
	for _, st := range role.Statements {
		for _, action := range st.Actions {
			if grant := st.Sid + " " + action; whole && !forOthers(grant) && !used[grant] {
				errs = append(errs, fmt.Errorf("statement %s allows %s, which no call used", st.Sid, action))
			}
		}
	}
	return errors.Join(errs...)
}

// grants are a role's statements in one deployment of the suite's store, named by n.
type grants struct {
	m    *infra.Manifest
	role infra.Role
	cat  *catalog.Catalog
	n    catalog.Naming
	d    infra.Deployment
}

// check marks in used each statement and action that allows a call, in denied each call
// that no statement allows, and in foreign each statement that allows what a call would
// name elsewhere.
func (g grants) check(calls []emulatortest.Call, used, denied, foreign map[string]bool) error {
	checked := map[string]bool{} // an action and a resource name; calls repeat them often
	for _, c := range calls {
		action, arn, err := g.request(c)
		if err != nil {
			return err
		}
		for _, name := range c.Resources {
			if checked[action+" "+name] {
				continue
			}
			checked[action+" "+name] = true
			sids, aerr := g.m.Allowing(g.role, g.d, action, arn+name, nil)
			if aerr != nil {
				return aerr
			}
			if len(sids) == 0 {
				denied[action+" on "+g.shown(arn+name)] = true
			}
			for _, sid := range sids {
				used[sid+" "+action] = true
			}
			if err = g.checkElsewhere(action, arn, name, foreign); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkElsewhere marks in foreign each statement that allows action on what a call that names
// the resource name would name elsewhere. arn is the call's ARN less the name.
func (g grants) checkElsewhere(action, arn, name string, foreign map[string]bool) error {
	others, err := g.elsewhere(action, name)
	if err != nil {
		return err
	}
	for other, why := range others {
		sids, aerr := g.m.Allowing(g.role, g.d, action, arn+other, nil)
		if aerr != nil {
			return aerr
		}
		for _, sid := range sids {
			foreign[fmt.Sprintf("statement %s allows %s on %s, %s", sid, action, g.shown(arn+other), why)] = true
		}
	}
	return nil
}

// elsewhere returns, with why the role must not allow each, what a call of action that names
// the resource name would name for another deployment and for another store, and for a
// DynamoDB call, the store's stream tables, which the projector reads only through the
// mapping.
func (g grants) elsewhere(action, name string) (map[string]string, error) {
	rest, ok := strings.CutPrefix(name, g.n.Base())
	if !ok {
		return nil, fmt.Errorf("a call names %s, which isn't a resource of store %s", name, g.n.Base())
	}
	out := map[string]string{
		catalog.Naming{Prefix: "dev-", Store: g.n.Store}.Base() + rest:     "another deployment's",
		catalog.Naming{Prefix: g.n.Prefix, Store: "billing"}.Base() + rest: "another store's",
	}
	if strings.HasPrefix(action, "dynamodb:") {
		for _, st := range g.cat.Streams {
			out[g.n.StreamTable(st.Name)] = "a stream table"
		}
	}
	return out, nil
}

// shown is arn with ${prefix} for the test's prefix, the same in every test.
func (g grants) shown(arn string) string { return strings.Replace(arn, g.d.Prefix, "${prefix}", 1) }

// request returns a call's IAM action, and the ARN of the resources it names less their name.
func (g grants) request(c emulatortest.Call) (string, string, error) {
	api, op, _ := strings.Cut(c.Target, ".")
	var service, resource string
	switch api {
	case "DynamoDB_20120810":
		service, resource = "dynamodb", "table/"
	case "Firehose_20150804":
		service, resource = "firehose", "deliverystream/"
	default:
		return "", "", fmt.Errorf("call %s: no IAM service for %s", c.Target, api)
	}
	if len(c.Resources) == 0 {
		return "", "", fmt.Errorf("call %s names no resource", c.Target)
	}
	return service + ":" + op, "arn:" + g.d.Partition + ":" + service + ":" + g.d.Region + ":" + g.d.Account + ":" + resource, nil
}
