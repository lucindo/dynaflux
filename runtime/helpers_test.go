// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/lucindo/dynaflux/internal/emulatortest"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/runtime"
)

// sdkOptions returns the options of the DynamoDB and Firehose clients that Run builds, with
// the SDK's defaults mode set to mode, "" for none. Each client resolves its HTTP client
// and retryer from Run's configuration.
func sdkOptions(t *testing.T, mode aws.DefaultsMode) (dynamodb.Options, fh.Options) {
	t.Helper()
	cfg, err := runtime.AWSConfig(t.Context(), emulatortest.Region)
	if err != nil {
		t.Fatal(err)
	}
	// What LoadDefaultConfig found in ~/.aws or AWS_* never reaches a client.
	cfg.BaseEndpoint = new("http://127.0.0.1:1")
	cfg.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: emulatortest.AccessKeyID, SecretAccessKey: "fake"}, nil
	})
	cfg.DefaultsMode = mode
	return dynamodb.NewFromConfig(cfg).Options(), fh.NewFromConfig(cfg).Options()
}

// rowText returns a view row's values separated by spaces.
func rowText(row []expr.Value) string {
	parts := make([]string, len(row))
	for i, v := range row {
		switch v.Kind() { //nolint:exhaustive // the kinds of the columns the tests read
		case expr.KindString:
			parts[i] = v.AsString()
		case expr.KindUint:
			parts[i] = strconv.FormatUint(v.AsUint(), 10)
		case expr.KindMicros:
			parts[i] = time.UnixMicro(v.AsMicros()).UTC().Format(time.RFC3339Nano)
		default:
			parts[i] = fmt.Sprintf("kind %d", v.Kind())
		}
	}
	return strings.Join(parts, " ")
}

// jsonLines returns the log lines in s, each decoded.
func jsonLines(t *testing.T, s string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(s) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q isn't JSON: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}
