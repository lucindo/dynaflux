// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"reflect"
)

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
		"s3":         names(reflect.TypeFor[S3]()),
		"dynamodb":   append(names(reflect.TypeFor[DynamoDB]()), names(reflect.TypeFor[streamsAPI]())...),
		"cloudwatch": append(names(reflect.TypeFor[CloudWatch]()), names(reflect.TypeFor[MetricLister]())...),
		"athena":     names(reflect.TypeFor[Athena]()),
		"firehose":   names(reflect.TypeFor[Firehose]()),
		"lambda":     names(reflect.TypeFor[Lambda]()),
	}
}
