// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Variables that Lambda sets.
const (
	envRuntimeAPI     = "AWS_LAMBDA_RUNTIME_API"
	envRegion         = "AWS_REGION"
	envMaxConcurrency = "AWS_LAMBDA_MAX_CONCURRENCY"
	envLogLevel       = "AWS_LAMBDA_LOG_LEVEL"
)

// env is the function's configuration, from its environment.
type env struct {
	mode   string // catalog.EnvModeProjector or catalog.EnvModeSetup
	prefix string
	region string
	// archiveSchema is DYNAFLUX_ARCHIVE_SCHEMA, "" when unset: the templates remove it
	// in regions that don't archive.
	archiveSchema string
	logLevel      slog.Level
}

// readEnv reads the function's variables with getenv and checks them against cat. Only
// projector and setup modes start yet.
func readEnv(cat *catalog.Catalog, getenv func(string) string) (env, error) {
	if getenv(envRuntimeAPI) == "" {
		return env{}, fmt.Errorf("runtime: %s isn't set: the runtime binary runs only on AWS Lambda", envRuntimeAPI)
	}
	mode := getenv(catalog.EnvMode)
	switch mode {
	case catalog.EnvModeProjector, catalog.EnvModeSetup:
	case catalog.EnvModeQuery, catalog.EnvModeBackfill:
		return env{}, fmt.Errorf("runtime: %s=%s needs a later Dynaflux release", catalog.EnvMode, mode)
	default:
		return env{}, fmt.Errorf("runtime: %s is %q; want %s or %s", catalog.EnvMode, mode, catalog.EnvModeProjector, catalog.EnvModeSetup)
	}
	if s := getenv(catalog.EnvStore); s != cat.Store {
		return env{}, fmt.Errorf("runtime: %s is %q, but the catalog's store is %s: deploy the templates and the function "+
			"from the same dynaflux generate", catalog.EnvStore, s, cat.Store)
	}
	e := env{
		mode:          mode,
		prefix:        getenv(catalog.EnvPrefix),
		region:        getenv(envRegion),
		archiveSchema: getenv(catalog.EnvArchiveSchema),
		logLevel:      logLevel(getenv(envLogLevel)),
	}
	if !catalog.ValidPrefix(e.prefix) {
		return env{}, fmt.Errorf("runtime: %s is %q; use lowercase letters, digits and -, starting with a letter "+
			"and ending with -, such as prod-, or \"\" for none", catalog.EnvPrefix, e.prefix)
	}
	if e.region == "" {
		return env{}, fmt.Errorf("runtime: %s isn't set", envRegion)
	}
	// aws-lambda-go runs this many invocations at once in one process (Lambda Managed
	// Instances), which AWS doesn't document to freeze as on-demand Lambda does (ADR 0010).
	if n, err := strconv.Atoi(getenv(envMaxConcurrency)); err == nil && n > 1 && mode == catalog.EnvModeProjector {
		return env{}, fmt.Errorf("runtime: %s is %d: the projector needs on-demand Lambda, one invocation per process",
			envMaxConcurrency, n)
	}
	return e, nil
}

// logLevel returns the slog level of Lambda's application log level.
func logLevel(s string) slog.Level {
	switch s {
	case "TRACE", "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR", "FATAL":
		return slog.LevelError
	}
	return slog.LevelInfo
}
