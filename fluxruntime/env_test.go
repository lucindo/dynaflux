// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxruntime_test

import (
	"log/slog"
	"maps"
	"testing"

	"github.com/lucindo/dynaflux/fluxruntime"
	"github.com/lucindo/dynaflux/internal/catalog"
)

// lambdaEnv is the environment Lambda and the templates give a projector of store app.
func lambdaEnv() map[string]string {
	return map[string]string{
		"AWS_LAMBDA_RUNTIME_API": "127.0.0.1:9001",
		"AWS_REGION":             region,
		"DYNAFLUX_MODE":          "projector",
		"DYNAFLUX_STORE":         "app",
		"DYNAFLUX_PREFIX":        "prod-",
	}
}

// The function checks its environment before Lambda's first invocation, so a function that
// can't serve fails its Init phase, loudly.
func TestReadEnv(t *testing.T) {
	t.Parallel()
	cat := compile(t, false).Catalog()
	data, merr := catalog.Marshal(cat)
	if merr != nil {
		t.Fatal(merr)
	}
	hash := catalog.Hash(data)
	prod := fluxruntime.Env{Mode: "projector", Region: region, Prefix: "prod-"}
	tests := []struct {
		name string
		set  map[string]string // "" deletes the variable
		want fluxruntime.Env
		err  string
	}{
		{"projector", nil, prod, ""},
		{"no prefix", map[string]string{"DYNAFLUX_PREFIX": ""}, fluxruntime.Env{Mode: "projector", Region: region}, ""},
		{"archive schema", map[string]string{"DYNAFLUX_ARCHIVE_SCHEMA": "sha256:0a1b"},
			fluxruntime.Env{Mode: "projector", Region: region, Prefix: "prod-", ArchiveSchema: "sha256:0a1b"}, ""},
		{"one invocation at a time", map[string]string{"AWS_LAMBDA_MAX_CONCURRENCY": "1"}, prod, ""},
		{"not on Lambda", map[string]string{"AWS_LAMBDA_RUNTIME_API": ""}, fluxruntime.Env{},
			"fluxruntime: AWS_LAMBDA_RUNTIME_API isn't set: the runtime binary runs only on AWS Lambda"},
		{"no mode", map[string]string{"DYNAFLUX_MODE": ""}, fluxruntime.Env{}, `fluxruntime: DYNAFLUX_MODE is ""; want projector, setup, query or backfill`},
		{"unknown mode", map[string]string{"DYNAFLUX_MODE": "Projector"}, fluxruntime.Env{},
			`fluxruntime: DYNAFLUX_MODE is "Projector"; want projector, setup, query or backfill`},
		{"setup", map[string]string{"DYNAFLUX_MODE": "setup"}, fluxruntime.Env{Mode: "setup", Region: region, Prefix: "prod-"}, ""},
		// Its requests are idempotent and write no view rows, so S9's freeze doesn't bind it.
		{"setup on Lambda Managed Instances", map[string]string{"DYNAFLUX_MODE": "setup", "AWS_LAMBDA_MAX_CONCURRENCY": "2"},
			fluxruntime.Env{Mode: "setup", Region: region, Prefix: "prod-"}, ""},
		{"setup of another store", map[string]string{"DYNAFLUX_MODE": "setup", "DYNAFLUX_STORE": "shop"}, fluxruntime.Env{},
			`fluxruntime: DYNAFLUX_STORE is "shop", but the catalog's store is app: ` +
				"deploy the templates and the function from the same dynaflux generate"},
		// The engine is safe for concurrent use.
		{"query", map[string]string{"DYNAFLUX_MODE": "query", "DYNAFLUX_CATALOG_HASH": hash, "AWS_LAMBDA_MAX_CONCURRENCY": "8"},
			fluxruntime.Env{Mode: "query", Region: region, Prefix: "prod-"}, ""},
		// query.yaml and the binary must come from one generate run.
		{"query of another catalog", map[string]string{"DYNAFLUX_MODE": "query", "DYNAFLUX_CATALOG_HASH": "0a1b"}, fluxruntime.Env{},
			`fluxruntime: DYNAFLUX_CATALOG_HASH is "0a1b", but the binary's catalog is ` + hash +
				": deploy query.yaml and the function from the same dynaflux generate"},
		{"query without the catalog's hash", map[string]string{"DYNAFLUX_MODE": "query"}, fluxruntime.Env{},
			`fluxruntime: DYNAFLUX_CATALOG_HASH is "", but the binary's catalog is ` + hash +
				": deploy query.yaml and the function from the same dynaflux generate"},
		// A step holds three quarters of the function's memory in staged records.
		{"backfill", map[string]string{"DYNAFLUX_MODE": "backfill", "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "3008"},
			fluxruntime.Env{Mode: "backfill", Region: region, Prefix: "prod-", Memory: 2_365_587_456}, ""},
		{"backfill without its memory", map[string]string{"DYNAFLUX_MODE": "backfill"}, fluxruntime.Env{},
			`fluxruntime: AWS_LAMBDA_FUNCTION_MEMORY_SIZE is "", not the function's memory in MB`},
		{"another store", map[string]string{"DYNAFLUX_STORE": "shop"}, fluxruntime.Env{},
			`fluxruntime: DYNAFLUX_STORE is "shop", but the catalog's store is app: ` +
				"deploy the templates and the function from the same dynaflux generate"},
		{"bad prefix", map[string]string{"DYNAFLUX_PREFIX": "Prod"}, fluxruntime.Env{}, `fluxruntime: DYNAFLUX_PREFIX is "Prod"; use lowercase letters, ` +
			`digits and -, starting with a letter and ending with -, such as prod-, or "" for none`},
		{"no region", map[string]string{"AWS_REGION": ""}, fluxruntime.Env{}, "fluxruntime: AWS_REGION isn't set"},
		{"Lambda Managed Instances", map[string]string{"AWS_LAMBDA_MAX_CONCURRENCY": "2"}, fluxruntime.Env{},
			"fluxruntime: AWS_LAMBDA_MAX_CONCURRENCY is 2: the projector needs on-demand Lambda, one invocation per process"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			vars := lambdaEnv()
			maps.Copy(vars, tt.set)
			got, err := fluxruntime.ReadEnv(cat, vars)
			if msg := errText(err); got != tt.want || msg != tt.err {
				t.Errorf("ReadEnv = %+v, %q; want %+v, %q", got, msg, tt.want, tt.err)
			}
		})
	}
}

// Lambda's application log level sets the level of the function's own lines.
func TestReadEnvLogLevel(t *testing.T) {
	t.Parallel()
	cat := compile(t, false).Catalog()
	for level, want := range map[string]slog.Level{
		"TRACE": slog.LevelDebug, "DEBUG": slog.LevelDebug, "INFO": slog.LevelInfo, "WARN": slog.LevelWarn,
		"ERROR": slog.LevelError, "FATAL": slog.LevelError, "": slog.LevelInfo,
	} {
		vars := lambdaEnv()
		vars["AWS_LAMBDA_LOG_LEVEL"] = level
		e, err := fluxruntime.ReadEnv(cat, vars)
		if err != nil || e.LogLevel != want {
			t.Errorf("AWS_LAMBDA_LOG_LEVEL=%s: level %v, %v; want %v", level, e.LogLevel, err, want)
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
