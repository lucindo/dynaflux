// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMain points the AWS SDK at a closed loopback port, with fake keys and no shared
// configuration, so that a test reaching the real connection never calls AWS.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "dynaflux-cli")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	empty := filepath.Join(dir, "empty")
	err = os.WriteFile(empty, nil, 0o600)
	for k, v := range map[string]string{
		"AWS_ENDPOINT_URL":            closedEndpoint,
		"AWS_CONFIG_FILE":             empty,
		"AWS_SHARED_CREDENTIALS_FILE": empty,
		"AWS_ACCESS_KEY_ID":           "fake",
		"AWS_SECRET_ACCESS_KEY":       "fake",
		"AWS_EC2_METADATA_DISABLED":   "true",
	} {
		if err == nil {
			err = os.Setenv(k, v)
		}
	}
	for _, k := range []string{"AWS_PROFILE", "AWS_SESSION_TOKEN"} {
		if err == nil {
			err = os.Unsetenv(k)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	code := m.Run()
	if err = os.RemoveAll(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

const closedEndpoint = "http://127.0.0.1:1"

func TestUnitTestsNeverReachAWS(t *testing.T) {
	t.Parallel()
	got := run(writeProject(t, "deployed"), "query", "PageStats", "-region", "us-east-1")
	if got.status != 2 || !strings.Contains(got.stderr, closedEndpoint) {
		t.Errorf("got %+v, want exit 2 with an error that names %s", got, closedEndpoint)
	}
}
