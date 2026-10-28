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
	envMemory         = "AWS_LAMBDA_FUNCTION_MEMORY_SIZE"
)

// backfillMemoryShare is the share of the function's memory that a backfill step spends on
// staged records: the rest is the runtime's, the SDK's and the merge's buffers.
const (
	backfillMemoryShare = 0.75
	mebibyte            = 1 << 20
)

// env is the function's configuration, from its environment.
type env struct {
	mode   string // a value of catalog.EnvMode
	prefix string
	region string
	// archiveSchema is DYNAFLUX_ARCHIVE_SCHEMA, "" when unset: the templates remove it
	// in regions that don't archive.
	archiveSchema string
	logLevel      slog.Level
	// memory is the bytes a backfill step may hold; 0 in the other modes.
	memory int64
}

// readEnv reads the function's variables with getenv and checks them against cat.
func readEnv(cat *catalog.Catalog, getenv func(string) string) (env, error) {
	if getenv(envRuntimeAPI) == "" {
		return env{}, fmt.Errorf("runtime: %s isn't set: the runtime binary runs only on AWS Lambda", envRuntimeAPI)
	}
	mode := getenv(catalog.EnvMode)
	switch mode {
	case catalog.EnvModeProjector, catalog.EnvModeSetup, catalog.EnvModeQuery, catalog.EnvModeBackfill:
	default:
		return env{}, fmt.Errorf("runtime: %s is %q; want %s, %s, %s or %s", catalog.EnvMode, mode,
			catalog.EnvModeProjector, catalog.EnvModeSetup, catalog.EnvModeQuery, catalog.EnvModeBackfill)
	}
	if s := getenv(catalog.EnvStore); s != cat.Store {
		return env{}, fmt.Errorf("runtime: %s is %q, but the catalog's store is %s: deploy the templates and the function "+
			"from the same dynaflux generate", catalog.EnvStore, s, cat.Store)
	}
	if mode == catalog.EnvModeQuery {
		if err := checkCatalogHash(cat, getenv(catalog.EnvCatalogHash)); err != nil {
			return env{}, err
		}
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
	if mode == catalog.EnvModeBackfill {
		mb, err := strconv.Atoi(getenv(envMemory))
		if err != nil || mb <= 0 {
			return env{}, fmt.Errorf("runtime: %s is %q, not the function's memory in MB", envMemory, getenv(envMemory))
		}
		e.memory = int64(float64(mb) * mebibyte * backfillMemoryShare)
	}
	return e, nil
}

// checkCatalogHash checks that hash, the query function's DYNAFLUX_CATALOG_HASH, names cat.
// query.yaml deploys with the application, after regional.yaml, so a function from another
// generate would serve another catalog's queries (ADR 0018).
func checkCatalogHash(cat *catalog.Catalog, hash string) error {
	data, err := catalog.Marshal(cat)
	if err != nil {
		return err
	}
	if own := catalog.Hash(data); hash != own {
		return fmt.Errorf("runtime: %s is %q, but the binary's catalog is %s: deploy query.yaml and the function from "+
			"the same dynaflux generate", catalog.EnvCatalogHash, hash, own)
	}
	return nil
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
