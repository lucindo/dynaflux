// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// lockProject writes dynaflux.lock in dir as dynaflux generate would.
func lockProject(t *testing.T, dir string) {
	t.Helper()
	data, err := catalog.MarshalLock(compile(t, dir).Lock)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "dynaflux.lock"), data, 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestDevRefuses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		lock bool
		args []string
		want result
	}{
		{
			"a lock that generate would change", false, []string{"dev", "-endpoint", closedEndpoint},
			result{1, "", "dynaflux dev: dynaflux.lock is out of date:\n  page_daily@v1: new\n  page_users@v1: new\n" +
				"  hint: run dynaflux generate, which the application needs too, then dynaflux dev\n"},
		},
		{
			"an endpoint on another host", true, []string{"dev", "-endpoint", "http://10.0.0.1:8000"},
			result{2, "", "dynaflux dev: -endpoint http://10.0.0.1:8000: 10.0.0.1 isn't a loopback address\nRun 'dynaflux help dev' for usage.\n"},
		},
		{
			"an argument", true, []string{"dev", "page_daily"},
			result{2, "", "dynaflux dev: unexpected arguments: page_daily\nRun 'dynaflux help dev' for usage.\n"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := writeProject(t, "dev")
			if tt.lock {
				lockProject(t, dir)
			}
			if got := run(dir, tt.args...); got != tt.want {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

// Without DynamoDB Local, dev says how to start it, at the port of -endpoint.
func TestDevWithoutDynamoDBLocal(t *testing.T) {
	t.Parallel()
	dir := writeProject(t, "dev")
	lockProject(t, dir)
	got := run(dir, "dev", "-endpoint", closedEndpoint)
	head := "store devshop  prefix (none)  endpoint http://127.0.0.1:1  region us-east-1\n" +
		"dynaflux dev: DynamoDB Local doesn't answer at http://127.0.0.1:1: "
	tail := "\n  hint: start it with docker run --rm -p 127.0.0.1:1:8000 -e DDB_LOCAL_TELEMETRY=0 amazon/dynamodb-local:3.3.2 -jar DynamoDBLocal.jar -sharedDb -inMemory -disableTelemetry\n"
	if got.status != 2 || got.stdout != "" || !strings.HasPrefix(got.stderr, head) || !strings.HasSuffix(got.stderr, tail) {
		t.Errorf("got %+v\nwant exit 2 with stderr\n%s…%s", got, head, tail)
	}
}

// The command in dev's help starts DynamoDB Local as scripts/emulators/compose.yaml does for
// the integration tests: the same tag, bound to loopback, in memory, with telemetry off by
// both the variable and the flag. -sharedDb is dev's own: it reads the tables with any key.
func TestDevHelpStartsTheTestedImage(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("..", "..", "scripts", "emulators", "compose.yaml"))
	must(t, err)
	var compose struct {
		Services map[string]struct {
			Image       string
			Command     []string
			Environment map[string]string
			Ports       []string
		}
	}
	must(t, yaml.Unmarshal(data, &compose))
	ddb := compose.Services["dynamodb"]
	image, _, _ := strings.Cut(ddb.Image, "@")
	if len(ddb.Ports) != 1 || len(ddb.Environment) != 1 {
		t.Fatalf("compose.yaml's dynamodb service: %+v; want one port and one variable", ddb)
	}
	want := []string{"docker", "run", "--rm", "-p", ddb.Ports[0]}
	for k, v := range ddb.Environment {
		want = append(want, "-e", k+"="+v)
	}
	want = append(append(want, image), ddb.Command...)

	help := run(t.TempDir(), "help", "dev").stdout
	i := strings.Index(help, "\n  docker run ")
	if i < 0 {
		t.Fatalf("dev's help has no docker run line:\n%s", help)
	}
	line, _, _ := strings.Cut(help[i+3:], "\n")
	got := slices.DeleteFunc(strings.Fields(line), func(f string) bool { return f == "-sharedDb" })
	if !slices.Equal(got, want) {
		t.Errorf("dev's help starts DynamoDB Local with\n%q\nwant, as compose.yaml and with -sharedDb,\n%q", got, want)
	}
}
