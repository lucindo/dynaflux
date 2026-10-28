// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package config loads dynaflux.yaml, the configuration of one Dynaflux store, as
// docs/spec/config.md specifies. It finds the file, decodes it strictly, applies the
// defaults, runs every check that needs no SQL, and expands the sql patterns.
//
// Checks that need the SQL definitions (DF6403, DF6407, DF6503, DF6504, DF6612, and DF6110
// for names that include a stream or view) belong to the compiler; the positions they point
// at are kept here.
package config

import (
	"fmt"
	"io/fs"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
)

// FileName is the name of the config file. The directory that holds it is the project root.
const FileName = "dynaflux.yaml"

// Config is a checked dynaflux.yaml with every default applied.
type Config struct {
	Store  string
	Prefix string // empty for no prefix
	// Regions are in file order; the catalog sorts them.
	Regions []string
	// RegionsPos is where regions is set, for DF6017.
	RegionsPos diag.Pos
	SQL        []Pattern
	Go         Go
	Runtime    Runtime
	Infra      Infra
	Defaults   Defaults
	Tables     []Table // in file order
	Archive    Archive
	HTTP       HTTP
}

// Naming returns the resource namer for the store and prefix.
func (c *Config) Naming() catalog.Naming {
	return catalog.Naming{Prefix: c.Prefix, Store: c.Store}
}

// Go configures the generated Go package.
type Go struct {
	Package string
	Out     string // cleaned, relative to the project root
}

// Runtime configures the Lambda functions.
type Runtime struct {
	Out            string // cleaned, relative to the project root
	Arch           Arch
	TimeoutSeconds int
	// PruneWindowSeconds is how long the projector keeps a delivery unit's high-water mark
	// in a view row after the unit's progress has passed it.
	PruneWindowSeconds    int
	MemoryMB              int
	BatchingWindowSeconds int
	Split                 Split
}

// Arch is a Lambda architecture.
type Arch string

// Architectures.
const (
	ArchARM64 Arch = "arm64"
	ArchX8664 Arch = "x86_64"
)

// Split says how projector functions are split.
type Split string

// Splits.
const (
	SplitStore  Split = "store"  // One projector function for the store.
	SplitStream Split = "stream" // One projector function per stream.
)

// Infra configures the generated infrastructure templates.
type Infra struct {
	Format InfraFormat
	Out    string // cleaned, relative to the project root
}

// InfraFormat is a template format.
type InfraFormat string

// InfraFormatSAM is CloudFormation/SAM, the only format.
const InfraFormatSAM InfraFormat = "sam"

// Defaults apply to every table, view and stream that doesn't set its own value.
type Defaults struct {
	Capacity     Capacity
	ViewMode     catalog.Mode
	LogRetention catalog.Retention
	// LogRetentionPos is where defaults.log_retention is set; zero when it isn't.
	LogRetentionPos diag.Pos
}

// Capacity is how a table is billed and scaled.
type Capacity struct {
	Mode        CapacityMode
	Autoscaling *Autoscaling // set exactly when Mode is provisioned
}

// CapacityMode is a DynamoDB capacity mode.
type CapacityMode string

// Capacity modes.
const (
	CapacityOnDemand    CapacityMode = "on_demand"
	CapacityProvisioned CapacityMode = "provisioned"
)

// Autoscaling bounds the provisioned capacity of one table in one region.
type Autoscaling struct {
	Read  Scaling
	Write Scaling
}

// Scaling is the auto scaling range of read or write capacity units.
type Scaling struct {
	Min           int
	Max           int
	TargetPercent int
}

// Table is an entry of tables: a capacity override for one stream or view.
type Table struct {
	Name     string
	Pos      diag.Pos // the key, for DF6407
	Capacity Capacity
}

// Archive places the archive of the streams.
type Archive struct {
	Enabled bool
	Catalog ArchiveCatalog
	// Regions are the archiving regions; archive.regions: all resolves to Config.Regions.
	Regions []string
	// AllRegions is set when archive.regions is all.
	AllRegions bool
	// RegionPos is where each of Regions is written, for DF6505: in archive.regions, or in
	// regions when archive.regions is all.
	RegionPos map[string]diag.Pos
	Events    catalog.ArchiveEvents
	Streams   []ArchiveStream // in file order
}

// ArchiveCatalog is where the archive's Iceberg tables are cataloged.
type ArchiveCatalog string

// Archive catalogs.
const (
	ArchiveCatalogGlue     ArchiveCatalog = "glue"
	ArchiveCatalogS3Tables ArchiveCatalog = "s3tables"
)

// ArchiveStream is an entry of archive.streams: an override of archive.enabled.
type ArchiveStream struct {
	Name       string
	Pos        diag.Pos // the key, for DF6503
	Enabled    bool
	EnabledPos diag.Pos // the enabled value, for DF6504
}

// HTTP configures the front door of queries annotated -- expose: http.
type HTTP struct {
	// Pos is where the http section is, for DF6612; zero without one.
	Pos         diag.Pos
	Frontend    Frontend
	Auth        Auth
	AllowPublic bool
	// The settings of each auth; nil when not set.
	JWT        *JWT
	Authorizer *Authorizer
	Cognito    *Cognito
	UsagePlan  *UsagePlan
	// MaxConcurrency is the query function's reserved concurrency; 0 when not set.
	MaxConcurrency int
	Domain         *Domain // nil when not set
}

// JWT is the JWT authorizer of auth: jwt.
type JWT struct {
	Issuer   string
	Audience []string
	Scopes   []string // empty: any token for the audience passes
}

// Authorizer is the Lambda authorizer of auth: lambda.
type Authorizer struct {
	Function string // a function of this name in every region
}

// Cognito is the user pool authorizer of auth: cognito.
type Cognito struct {
	UserPoolARN string
	Scopes      []string // access tokens that carry one of them pass
	AppClients  []string // ID tokens whose aud is one of them pass
}

// UsagePlan limits the API keys of auth: api_key. A zero field sets no limit.
type UsagePlan struct {
	Rate        int // requests per second
	Burst       int
	QuotaPerDay int
}

// Frontend is an HTTP front door.
type Frontend string

// Front doors.
const (
	FrontendFunctionURL Frontend = "function_url"
	FrontendHTTPAPI     Frontend = "http_api"
	FrontendRESTAPI     Frontend = "rest_api"
)

// Auth is how the front door authenticates callers.
type Auth string

// Auth methods.
const (
	AuthIAM     Auth = "iam"
	AuthNone    Auth = "none"
	AuthJWT     Auth = "jwt"
	AuthLambda  Auth = "lambda"
	AuthAPIKey  Auth = "api_key"
	AuthCognito Auth = "cognito"
)

// Domain is the custom domain of the front door.
type Domain struct {
	Name    string
	Routing Routing
}

// Routing is how the custom domain routes requests across regions.
type Routing string

// RoutingLatency is Route 53 latency routing, the only routing.
const RoutingLatency Routing = "latency"

// Load reads and checks dynaflux.yaml at the root of fsys, the project root. The config is
// nil when any diagnostic is an error. The diagnostics are sorted.
func Load(fsys fs.FS) (*Config, []diag.Diagnostic) {
	src, err := fs.ReadFile(fsys, FileName)
	if err != nil {
		return nil, []diag.Diagnostic{{
			Pos:      pos(1, 1),
			Severity: diag.SeverityError,
			Code:     "DF6002",
			Message:  fmt.Sprintf("can't read %s: %v", FileName, err),
			Hint:     "check the path and its permissions",
		}}
	}
	root, ds := parse(src)
	if diag.HasErrors(ds) {
		diag.Sort(ds)
		return nil, ds
	}
	cfg, ds := decode(root)
	diag.Sort(ds)
	if diag.HasErrors(ds) {
		return nil, ds
	}
	return cfg, ds
}
