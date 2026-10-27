// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awssetup

import "reflect"

// Operations returns the operations that the adapters can send, by IAM service prefix.
func Operations() map[string][]string {
	names := func(t reflect.Type) []string {
		out := make([]string, t.NumMethod())
		for i := range out {
			out[i] = t.Method(i).Name
		}
		return out
	}
	return map[string][]string{
		"dynamodb": names(reflect.TypeFor[dynamoAPI]()),
		"glue":     names(reflect.TypeFor[glueAPI]()),
		"athena":   names(reflect.TypeFor[athenaAPI]()),
	}
}
