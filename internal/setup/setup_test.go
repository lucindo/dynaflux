// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

	"github.com/lucindo/dynaflux/internal/setup"
)

// archiveStatement matches the only statements the setup function may run: it never drops
// or retypes anything.
const archiveStatement = "^(CREATE TABLE |ALTER TABLE [^ ]+ ADD COLUMNS \\(|CREATE OR REPLACE VIEW )"

// fakeAWS is Athena and Glue over live Glue tables by name, which it never changes. It
// records the calls that would change something, and fails the test on any statement
// that archiveStatement doesn't match.
type fakeAWS struct {
	t      *testing.T
	tables map[string]setup.GlueTable
	calls  []string
	// execErr, when set, returns the error of each statement.
	execErr func(sql string) error
	// denied is how many calls of each method are refused before the grants apply.
	denied int
	tries  map[string]int
}

// refuse returns an AccessDenied error for the first denied calls of method.
func (f *fakeAWS) refuse(method string) error {
	if f.tries == nil {
		f.tries = map[string]int{}
	}
	f.tries[method]++
	if f.tries[method] <= f.denied {
		return fmt.Errorf("%w: not authorized to perform %s", setup.ErrAccessDenied, method)
	}
	return nil
}

func (f *fakeAWS) Exec(_ context.Context, workgroup, sql string) error {
	if err := f.refuse("athena:StartQueryExecution"); err != nil {
		return err
	}
	if !regexp.MustCompile(archiveStatement).MatchString(sql) {
		f.t.Errorf("statement outside the allowed set:\n%s", sql)
	}
	f.calls = append(f.calls, "athena "+workgroup+"\n"+sql)
	if f.execErr != nil {
		return f.execErr(sql)
	}
	return nil
}

func (f *fakeAWS) CreateDatabase(_ context.Context, name, location string) error {
	if err := f.refuse("glue:CreateDatabase"); err != nil {
		return err
	}
	f.calls = append(f.calls, "glue database "+name+" at "+location)
	return nil
}

func (f *fakeAWS) Table(_ context.Context, db, table string) (setup.GlueTable, bool, error) {
	if err := f.refuse("glue:GetTable"); err != nil {
		return setup.GlueTable{}, false, err
	}
	if db != database {
		f.t.Errorf("table %s read from database %s", table, db)
	}
	t, ok := f.tables[table]
	return t, ok, nil
}

func (f *fakeAWS) PutOptimizer(_ context.Context, db, table string, o setup.Optimizer) error {
	if err := f.refuse("glue:CreateTableOptimizer"); err != nil {
		return err
	}
	f.calls = append(f.calls, fmt.Sprintf("glue optimizer %s.%s\n%+v", db, table, o))
	return nil
}

const database = "prod_analytics"

// props parses properties as CloudFormation sends them.
func props(t *testing.T, data string) map[string]any {
	t.Helper()
	var p map[string]any
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// A delete never calls AWS: the handler's nil interfaces would panic.
func TestDeleteDoesNothing(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{setup.TypeStreamARN, setup.TypeArchiveSchema, setup.TypeArchiveMode, "Custom::Other"} {
		h := setup.Handler{}
		resp, err := h.Handle(t.Context(), setup.Request{Type: typ, Action: setup.ActionDelete, PhysicalID: "id-1"})
		if err != nil || resp.PhysicalID != "id-1" || resp.Data != nil {
			t.Errorf("%s: got %+v, %v; want physical ID id-1 and no data", typ, resp, err)
		}
	}
}

// CloudFormation takes a new physical ID in an update's reply for a replacement.
func TestFailedUpdateKeepsPhysicalID(t *testing.T) {
	t.Parallel()
	h := setup.Handler{DynamoDB: &fakeDynamoDB{}}
	resp, err := h.Handle(t.Context(), setup.Request{
		Type: setup.TypeStreamARN, Action: setup.ActionUpdate, PhysicalID: streamARN,
		Props: map[string]any{"TableName": table},
	})
	if err == nil || resp.PhysicalID != streamARN {
		t.Errorf("got %+v, %v; want an error and physical ID %s", resp, err, streamARN)
	}
}

func TestUnknownResourceType(t *testing.T) {
	t.Parallel()
	h := setup.Handler{}
	_, err := h.Handle(t.Context(), setup.Request{Type: "Custom::Other", Action: setup.ActionCreate})
	if want := `unknown resource type "Custom::Other"`; err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
}
