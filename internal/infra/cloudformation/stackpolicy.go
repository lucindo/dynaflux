// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"encoding/json"
	"slices"

	"github.com/lucindo/dynaflux/internal/infra"
)

type stackPolicyStatement struct {
	Effect    string                         `json:"Effect"`
	Action    string                         `json:"Action"`
	Principal string                         `json:"Principal"`
	Resource  string                         `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// stackPolicy allows every update except replacing a Global Table, when the manifest marks
// the tables of s no-replace. A stack policy is set once, so it names the type rather than
// the tables, and so protects the stream tables added later too. Deleting them stays
// allowed: they are retained.
func stackPolicy(s infra.Scope) ([]byte, error) {
	statements := []stackPolicyStatement{{Effect: "Allow", Action: "Update:*", Principal: "*", Resource: "*"}}
	if slices.ContainsFunc(s.Tables, func(t infra.Table) bool { return t.NoReplace }) {
		statements = append(statements, stackPolicyStatement{
			Effect: "Deny", Action: "Update:Replace", Principal: "*", Resource: "*",
			Condition: map[string]map[string][]string{"StringEquals": {"ResourceType": {"AWS::DynamoDB::GlobalTable"}}},
		})
	}
	data, err := json.MarshalIndent(struct {
		Statement []stackPolicyStatement `json:"Statement"`
	}{statements}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
