// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package deployed

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

// Mapping is an event source mapping that reads a change stream.
type Mapping struct {
	UUID     string
	Function string // the name of the function it invokes
	State    string // such as Enabled or Disabled
	// LastResult is its LastProcessingResult: OK, No records processed, or PROBLEM: and why.
	LastResult string
}

// Delivering reports whether the mapping reads its stream, or soon will from its checkpoint:
// it is enabled, or being created, enabled or updated.
func (m Mapping) Delivering() bool {
	switch m.State {
	case "Creating", "Enabling", "Enabled", "Updating":
		return true
	}
	return false
}

// Mappings lists the event source mappings that read the change stream whose ARN is stream.
func Mappings(ctx context.Context, c Lambda, stream string) ([]Mapping, error) {
	var out []Mapping
	pages := lambda.NewListEventSourceMappingsPaginator(c, &lambda.ListEventSourceMappingsInput{EventSourceArn: &stream})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("deployed: list the event source mappings of %s: %w", stream, err)
		}
		for _, m := range page.EventSourceMappings {
			out = append(out, Mapping{
				UUID: aws.ToString(m.UUID), Function: functionName(aws.ToString(m.FunctionArn)),
				State: aws.ToString(m.State), LastResult: aws.ToString(m.LastProcessingResult),
			})
		}
	}
	return out, nil
}

// functionName returns the name in a function's ARN,
// arn:<partition>:lambda:<region>:<account>:function:<name>, which may end in a qualifier.
func functionName(arn string) string {
	_, rest, ok := strings.Cut(arn, ":function:")
	if !ok {
		return arn
	}
	name, _, _ := strings.Cut(rest, ":")
	return name
}
