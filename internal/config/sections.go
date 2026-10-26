// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"go/token"
	"math"
	"path"
	"slices"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
)

// Defaults and limits from docs/spec/config.md.
const (
	defaultGoPackage       = "flux"
	defaultGoOut           = "internal/flux"
	defaultRuntimeOut      = "cmd/dynaflux-runtime"
	defaultInfraOut        = "infra/dynaflux"
	defaultTimeoutSeconds  = 60
	defaultPruneWindow     = maxTimeoutSeconds + lifetimeMargin // safe for every timeout
	defaultMemoryMB        = 1024
	defaultBatchingSeconds = 1
	defaultLogRetentionDay = 2
	defaultTargetPercent   = 70

	maxTimeoutSeconds  = 900   // Lambda maximum
	lifetimeMargin     = 60    // for writes that land late and clock skew between hosts
	minMemoryMB        = 128   // Lambda minimum
	maxMemoryMB        = 10240 // Lambda maximum
	maxBatchingSeconds = 300   // event source mapping maximum
	minTargetPercent   = 20    // DynamoDB auto scaling range
	maxTargetPercent   = 90

	maxDomainName  = 253
	maxDomainLabel = 63

	hintDuration = "write a whole number and one unit, such as 36h, 2d or 1w, or write forever"
)

// decode reads the top-level mapping into a Config and runs the checks that need no SQL.
func decode(root *yaml.Node) (*Config, []diag.Diagnostic) {
	var d decoder
	top, _ := d.object("", root, at(root),
		"store", "prefix", "regions", "sql", "go", "runtime", "infra", "defaults", "tables", "archive", "http")
	c := &Config{}
	named := d.identity(top, c)
	d.regions(top, c)
	d.sqlPatterns(top, c)
	splitKnown := d.outputs(top, c)
	d.defaults(top, c)
	d.tables(top, c)
	d.archive(top, c)
	d.http(top, c)
	if named {
		d.storeNames(c, top.at("store"), splitKnown)
	}
	return c, d.diags
}

// identity reads store and prefix, and reports whether both are valid.
func (d *decoder) identity(top object, c *Config) bool {
	before := len(d.diags)
	d.require(top, "store", "analytics")
	if s, ok := d.str(top, "store", "for example store: analytics"); ok {
		if lowerDashed(s) && !strings.HasSuffix(s, "-") {
			c.Store = s
		} else {
			d.errorf(top.at("store"), "DF6101",
				"use lowercase letters, digits and -; start with a letter; don't end with -. For example analytics",
				"store is %q, which isn't a valid store name", s)
		}
	}
	if s, ok := d.str(top, "prefix", "for example prefix: prod-"); ok {
		if s == "" || lowerDashed(s) && strings.HasSuffix(s, "-") {
			c.Prefix = s
		} else {
			d.errorf(top.at("prefix"), "DF6102",
				"use lowercase letters, digits and -; start with a letter; end with -. For example prod-",
				"prefix is %q, which isn't a valid prefix", s)
		}
	}
	return len(d.diags) == before
}

// lowerDashed reports whether s starts with a lowercase letter and has only lowercase
// letters, digits and "-".
func lowerDashed(s string) bool {
	return s != "" && strings.ContainsRune(lower, rune(s[0])) && consists(s, lower+digits+"-")
}

func (d *decoder) regions(top object, c *Config) {
	d.require(top, "regions", "[us-east-1]")
	seen := map[string]bool{}
	for _, e := range d.list(top, "regions", "[us-east-1]", false) {
		if !isRegion(e.Value) {
			d.errorf(at(e), "DF6103", "use a code such as us-east-1", "%q isn't an AWS region code", e.Value)
			continue
		}
		if !d.repeated(seen, "regions", e.Value, e) {
			c.Regions = append(c.Regions, e.Value)
		}
	}
}

// isRegion reports whether s has the form of an AWS region code, ^[a-z]+(-[a-z]+)+-[0-9]+$.
func isRegion(s string) bool {
	parts := strings.Split(s, "-")
	const minParts = 3
	if len(parts) < minParts {
		return false
	}
	words, number := parts[:len(parts)-1], parts[len(parts)-1]
	return number != "" && consists(number, digits) &&
		!slices.ContainsFunc(words, func(w string) bool { return w == "" || !consists(w, lower) })
}

// outputs reads go, runtime and infra, and reports whether runtime.split is known.
func (d *decoder) outputs(top object, c *Config) bool {
	c.Go = Go{Package: defaultGoPackage, Out: defaultGoOut}
	c.Runtime = Runtime{
		Out:                   defaultRuntimeOut,
		Arch:                  ArchARM64,
		TimeoutSeconds:        defaultTimeoutSeconds,
		PruneWindowSeconds:    defaultPruneWindow,
		MemoryMB:              defaultMemoryMB,
		BatchingWindowSeconds: defaultBatchingSeconds,
		Split:                 SplitStore,
	}
	c.Infra = Infra{Format: InfraFormatSAM, Out: defaultInfraOut}

	g, gOK := d.object("go", top.vals["go"], top.at("go"), "package", "out")
	goOut := gOK && d.dir(g, "out", &c.Go.Out)
	if gOK {
		d.goPackage(g, &c.Go)
	}
	r, rOK := d.object("runtime", top.vals["runtime"], top.at("runtime"),
		"out", "arch", "timeout_seconds", "prune_window_seconds", "memory_mb", "batching_window_seconds", "split")
	runtimeOut := rOK && d.dir(r, "out", &c.Runtime.Out)
	splitKnown := rOK && d.runtime(r, &c.Runtime)
	i, iOK := d.object("infra", top.vals["infra"], top.at("infra"), "format", "out")
	infraOut := iOK && d.dir(i, "out", &c.Infra.Out)
	if iOK {
		enum(d, i, "format", &c.Infra.Format, InfraFormatSAM)
	}

	// Only known directories are compared, so an unrelated error hides no DF6303 or DF6304.
	var outs []output
	if goOut {
		outs = append(outs, output{"go.out", c.Go.Out, g})
	}
	if runtimeOut {
		outs = append(outs, output{"runtime.out", c.Runtime.Out, r})
	}
	if infraOut {
		outs = append(outs, output{"infra.out", c.Infra.Out, i})
	}
	d.distinctOutputs(outs)
	if goOut && runtimeOut {
		d.importable(c, g, r)
	}
	return splitKnown
}

func (d *decoder) goPackage(g object, dst *Go) {
	s, ok := d.str(g, "package", "for example package: flux")
	if !ok {
		return
	}
	if why := goPackageProblem(s); why != "" {
		d.errorf(g.at("package"), "DF6301", "use a short lowercase name such as flux", "go.package is %q: %s", s, why)
		return
	}
	dst.Package = s
}

// runtime reads the runtime mapping r, and reports whether runtime.split is known.
func (d *decoder) runtime(r object, rt *Runtime) bool {
	enum(d, r, "arch", &rt.Arch, ArchARM64, ArchX8664)
	timeoutOK := d.integer(r, "timeout_seconds", &rt.TimeoutSeconds,
		bounds{1, maxTimeoutSeconds, "Lambda functions run for 1 to 900 seconds"}) || !r.has("timeout_seconds")
	windowSet := d.integer(r, "prune_window_seconds", &rt.PruneWindowSeconds,
		bounds{1, catalog.MaxPruneWindowSeconds,
			fmt.Sprintf("the prune window is at most %d seconds (25 hours)", catalog.MaxPruneWindowSeconds)})
	if timeoutOK && windowSet {
		d.pruneWindow(r, rt)
	}
	d.integer(r, "memory_mb", &rt.MemoryMB,
		bounds{minMemoryMB, maxMemoryMB, "Lambda functions take 128 to 10240 MB of memory"})
	d.integer(r, "batching_window_seconds", &rt.BatchingWindowSeconds,
		bounds{0, maxBatchingSeconds, "event source mappings batch for 0 to 300 seconds"})
	return enum(d, r, "split", &rt.Split, SplitStore, SplitStream) || !r.has("split")
}

// pruneWindow checks runtime.prune_window_seconds against the timeout. Pruning is exact
// only while the window outlasts every invocation that may still be running.
func (d *decoder) pruneWindow(r object, rt *Runtime) {
	least := rt.TimeoutSeconds + lifetimeMargin
	switch {
	case rt.PruneWindowSeconds < least:
		d.errorf(r.at("prune_window_seconds"), "DF6305",
			fmt.Sprintf("set it to at least %d, or remove it to use the default, %d", least, defaultPruneWindow),
			"runtime.prune_window_seconds is %d, less than runtime.timeout_seconds + %d = %d: the projector could "+
				"drop a high-water mark that a running invocation still needs, and count some events twice",
			rt.PruneWindowSeconds, lifetimeMargin, least)
	case rt.PruneWindowSeconds < defaultPruneWindow:
		d.warnf(r.at("prune_window_seconds"), "DF6306",
			fmt.Sprintf("remove it to use the default, %d, which is safe for every timeout", defaultPruneWindow),
			"runtime.prune_window_seconds is %d, less than %d: if you later lower it with runtime.timeout_seconds in "+
				"one deploy, invocations started under the old timeout can outlive the new window",
			rt.PruneWindowSeconds, defaultPruneWindow)
	}
}

// goPackageProblem says why s can't be go.package, or returns "".
func goPackageProblem(s string) string {
	switch {
	case s == "" || !strings.ContainsRune(lower, rune(s[0])) || !consists(s, lower+digits):
		return "it must be lowercase letters and digits, starting with a letter"
	case token.IsKeyword(s):
		return "it is a Go keyword"
	case s == "main":
		return "a main package can't be imported"
	case s == "dynaflux" || s == "runtime":
		return "generated code imports the Dynaflux package of that name"
	}
	return ""
}

// dir sets dst to the cleaned output path at k, when it is valid. It reports whether dst
// then holds a valid directory, which is the default when k isn't set.
func (d *decoder) dir(o object, k string, dst *string) bool {
	if !o.has(k) {
		return true
	}
	// A quoted number is a valid directory name.
	s, ok := d.str(o, k, `quote the value if YAML reads it as another type, as in "123"`)
	if !ok {
		return false
	}
	var why string
	switch {
	case s == "":
		why = "it is empty"
	case strings.HasPrefix(s, "/"):
		why = "it is absolute"
	case strings.Contains(s, `\`):
		why = `it contains \`
	case slices.Contains(strings.Split(s, "/"), ".."):
		why = "it has a .. segment"
	case path.Clean(s) == ".":
		why = "it is the project root"
	default:
		*dst = path.Clean(s)
		return true
	}
	d.errorf(o.at(k), "DF6302", "use a directory below the project root, such as "+*dst, "%s is %q: %s", o.key(k), s, why)
	return false
}

// output is one of go.out, runtime.out and infra.out, with the mapping that may set it.
type output struct {
	key string
	dir string
	in  object
}

// pos returns where the output is set, or the zero Pos for a default.
func (o output) pos() diag.Pos {
	if n := o.in.vals["out"]; n != nil {
		return at(n)
	}
	return diag.Pos{}
}

// distinctOutputs reports DF6303 at the later of two outputs that share a directory.
func (d *decoder) distinctOutputs(outs []output) {
	for j, b := range outs {
		for _, a := range outs[:j] {
			if a.dir != b.dir {
				continue
			}
			first, second := a, b
			if later(a.pos(), b.pos()) {
				first, second = b, a
			}
			d.errorf(second.pos(), "DF6303", "give each output its own directory",
				"%s and %s are the same directory, %q", first.key, second.key, a.dir)
			break
		}
	}
}

func later(a, b diag.Pos) bool {
	return a.Line > b.Line || a.Line == b.Line && a.Col > b.Col
}

// importable reports DF6304 when runtime.out is outside the tree that may import go.out:
// Go allows importing a package below an internal directory only from inside the
// directory that holds it.
func (d *decoder) importable(c *Config, g, r object) {
	padded := "/" + c.Go.Out + "/"
	i := strings.LastIndex(padded, "/internal/")
	if i <= 0 {
		return
	}
	parent := padded[1:i]
	if c.Runtime.Out == parent || strings.HasPrefix(c.Runtime.Out, parent+"/") {
		return
	}
	p := r.at("out")
	if !r.has("out") {
		p = g.at("out")
	}
	d.errorf(p, "DF6304", fmt.Sprintf("move runtime.out inside %s, or move go.out", parent),
		"runtime.out %s can't import go.out %s, which only code inside %s may import", c.Runtime.Out, c.Go.Out, parent)
}

func (d *decoder) defaults(top object, c *Config) {
	c.Defaults = Defaults{
		Capacity:     Capacity{Mode: CapacityOnDemand},
		ViewMode:     catalog.ModeAsync,
		LogRetention: catalog.RetainFor(catalog.Interval{N: defaultLogRetentionDay, Unit: catalog.UnitDay}),
	}
	o, ok := d.object("defaults", top.vals["defaults"], top.at("defaults"),
		"capacity", "autoscaling", "view_mode", "log_retention")
	if !ok {
		return
	}
	c.Defaults.Capacity = d.capacity(o, false)
	enum(d, o, "view_mode", &c.Defaults.ViewMode, catalog.ModeAsync, catalog.ModeSync)
	if r, set := d.retention(o); set {
		c.Defaults.LogRetention, c.Defaults.LogRetentionPos = r, o.at("log_retention")
	}
}

// retention reads log_retention: forever, or a duration from 1 day to 100 years.
func (d *decoder) retention(o object) (catalog.Retention, bool) {
	s, ok := d.str(o, "log_retention", hintDuration)
	if !ok {
		return catalog.Retention{}, false
	}
	if s == "forever" {
		return catalog.RetainForever(), true
	}
	n, unit, ok := splitDuration(s)
	if !ok {
		d.errorf(o.at("log_retention"), "DF6401", hintDuration,
			"%s is %q, which isn't a duration", o.key("log_retention"), s)
		return catalog.Retention{}, false
	}
	// The catalog holds the bounds that LOG RETENTION shares.
	var r catalog.Retention
	if err := r.UnmarshalText([]byte(n + " " + string(unit))); err != nil {
		maxDays := catalog.UnitDay.Max()
		d.errorf(o.at("log_retention"), "DF6402", fmt.Sprintf("use at least 1d; for more than %d days, use forever", maxDays),
			"%s is %q; it must be between 1 day and %d days", o.key("log_retention"), s, maxDays)
		return catalog.Retention{}, false
	}
	return r, true
}

// splitDuration splits a duration such as 36h into its number and unit.
func splitDuration(s string) (string, catalog.Unit, bool) {
	if len(s) < 2 || s[0] == '0' || !consists(s[:len(s)-1], digits) {
		return "", "", false
	}
	var unit catalog.Unit
	switch s[len(s)-1] {
	case 'h':
		unit = catalog.UnitHour
	case 'd':
		unit = catalog.UnitDay
	case 'w':
		unit = catalog.UnitWeek
	default:
		return "", "", false
	}
	return s[:len(s)-1], unit, true
}

// capacity reads capacity and autoscaling from o. An entry of tables must set capacity.
func (d *decoder) capacity(o object, required bool) Capacity {
	c := Capacity{Mode: CapacityOnDemand}
	if required {
		d.require(o, "capacity", "on_demand")
	}
	known := enum(d, o, "capacity", &c.Mode, CapacityOnDemand, CapacityProvisioned) || !o.has("capacity") && !required
	if o.has("autoscaling") {
		c.Autoscaling = d.autoscaling(o)
	}
	switch {
	case !known:
	case c.Mode == CapacityProvisioned && !o.has("autoscaling"):
		d.errorf(o.at("capacity"), "DF6404",
			"add autoscaling: {read: {min: 5, max: 100}, write: {min: 5, max: 100}} with your own numbers",
			"%s is provisioned, but %s isn't set", o.key("capacity"), o.key("autoscaling"))
	case c.Mode == CapacityOnDemand && o.has("autoscaling"):
		d.errorf(o.at("autoscaling"), "DF6405", "remove autoscaling, or set capacity: provisioned",
			"%s is set, but capacity is on_demand", o.key("autoscaling"))
	}
	return c
}

func (d *decoder) autoscaling(o object) *Autoscaling {
	a, ok := d.object(o.key("autoscaling"), o.vals["autoscaling"], o.pos, "read", "write")
	if !ok {
		return nil
	}
	d.require(a, "read", "{min: 5, max: 100}")
	d.require(a, "write", "{min: 5, max: 100}")
	return &Autoscaling{Read: d.scaling(a, "read"), Write: d.scaling(a, "write")}
}

func (d *decoder) scaling(a object, k string) Scaling {
	s := Scaling{TargetPercent: defaultTargetPercent}
	if !a.has(k) {
		return s
	}
	o, ok := d.object(a.key(k), a.vals[k], a.pos, "min", "max", "target_percent")
	if !ok {
		return s
	}
	d.require(o, "min", "5")
	d.require(o, "max", "100")
	units := bounds{1, math.MaxInt, "capacity units start at 1"}
	minOK := d.integer(o, "min", &s.Min, units)
	maxOK := d.integer(o, "max", &s.Max, units)
	d.integer(o, "target_percent", &s.TargetPercent,
		bounds{minTargetPercent, maxTargetPercent, "DynamoDB auto scaling targets 20 to 90 percent utilization"})
	if minOK && maxOK && s.Min > s.Max {
		d.errorf(o.at("min"), "DF6406", "set min to at most max", "%s is %d, more than max %d", o.key("min"), s.Min, s.Max)
	}
	return s
}

func (d *decoder) tables(top object, c *Config) {
	for k, v := range pairs(d.mapping(top, "tables")) {
		if !d.scalarKey("tables", k, "for example page_daily: {capacity: on_demand}") {
			continue
		}
		e, ok := d.object("tables."+k.Value, v, at(k), "capacity", "autoscaling")
		if ok {
			c.Tables = append(c.Tables, Table{Name: k.Value, Pos: at(k), Capacity: d.capacity(e, true)})
		}
	}
}

func (d *decoder) archive(top object, c *Config) {
	c.Archive = Archive{
		Enabled: true,
		Catalog: ArchiveCatalogGlue,
		Regions: slices.Clone(c.Regions),
		Events:  catalog.ArchiveEventsAll,
	}
	o, ok := d.object("archive", top.vals["archive"], top.at("archive"), "enabled", "catalog", "regions", "events", "streams")
	if !ok {
		return
	}
	d.boolean(o, "enabled", &c.Archive.Enabled)
	enum(d, o, "catalog", &c.Archive.Catalog, ArchiveCatalogGlue, ArchiveCatalogS3Tables)
	all := d.archiveRegions(o, c)
	local := enum(d, o, "events", &c.Archive.Events, catalog.ArchiveEventsAll, catalog.ArchiveEventsLocal) &&
		c.Archive.Events == catalog.ArchiveEventsLocal
	if local && !all {
		d.errorf(o.at("events"), "DF6502", "set archive.regions: all, or archive.events: all",
			"archive.events is local, but archive.regions isn't all: events accepted in a region that doesn't archive would never reach the archive")
	}
	for k, v := range pairs(d.mapping(o, "streams")) {
		if !d.scalarKey("archive.streams", k, "for example page_views: {enabled: false}") {
			continue
		}
		e, eOK := d.object("archive.streams."+k.Value, v, at(k), "enabled")
		if !eOK {
			continue
		}
		s := ArchiveStream{Name: k.Value, Pos: at(k), EnabledPos: e.at("enabled")}
		d.require(e, "enabled", "false")
		d.boolean(e, "enabled", &s.Enabled)
		c.Archive.Streams = append(c.Archive.Streams, s)
	}
}

// archiveRegions reads archive.regions into c and reports whether it is all. An invalid
// value counts as all, so it causes no further errors.
func (d *decoder) archiveRegions(o object, c *Config) bool {
	n := o.vals["regions"]
	switch {
	case n == nil:
		return true
	case isString(n):
		if n.Value != "all" {
			d.errorf(at(n), "DF6009", "use all, or a list of entries from regions such as [us-east-1]",
				"archive.regions is %q; it must be all or a list of regions", n.Value)
		}
		return true
	case n.Kind != yaml.SequenceNode:
		d.typeError("archive.regions", n, "all or a list of regions", "for example regions: [us-east-1]")
		return true
	}
	c.Archive.Regions = nil
	seen := map[string]bool{}
	for _, e := range d.list(o, "regions", "[us-east-1]", true) {
		switch {
		case !slices.Contains(c.Regions, e.Value):
			// With regions in error, every entry would be reported.
			if len(c.Regions) > 0 {
				d.errorf(at(e), "DF6501", "add it to regions, or remove it from archive.regions",
					"archive.regions lists %q, which isn't in regions", e.Value)
			}
		case !d.repeated(seen, "archive.regions", e.Value, e):
			c.Archive.Regions = append(c.Archive.Regions, e.Value)
		}
	}
	return len(n.Content) == 0
}

func (d *decoder) http(top object, c *Config) {
	h := &c.HTTP
	*h = HTTP{Frontend: FrontendFunctionURL, Auth: AuthIAM}
	o, ok := d.object("http", top.vals["http"], top.at("http"), "frontend", "auth", "allow_public", "domain")
	if !ok {
		return
	}
	frontendOK := enum(d, o, "frontend", &h.Frontend, FrontendFunctionURL, FrontendHTTPAPI, FrontendRESTAPI) ||
		!o.has("frontend")
	authOK := enum(d, o, "auth", &h.Auth, AuthIAM, AuthNone, AuthJWT, AuthLambda, AuthAPIKey, AuthCognito) ||
		!o.has("auth")
	d.boolean(o, "allow_public", &h.AllowPublic)
	h.Domain = d.domain(o)
	if frontendOK && authOK {
		d.auth(o, h)
	}
	if authOK && h.Auth != AuthNone && h.AllowPublic {
		d.warnf(o.at("allow_public"), "DF6604", "remove allow_public",
			"http.allow_public: true has no effect, because http.auth is %s, not none", h.Auth)
	}
	if frontendOK && h.Frontend == FrontendFunctionURL && o.has("domain") {
		d.errorf(o.at("domain"), "DF6605",
			"function URLs have no custom domain option; use frontend: http_api for one hostname across regions",
			"http.domain is set, but http.frontend is function_url")
	}
}

// auth checks http.auth against the front door and allow_public.
func (d *decoder) auth(o object, h *HTTP) {
	supported := authsFor(h.Frontend)
	switch {
	case !slices.Contains(supported, h.Auth):
		d.errorf(o.at("auth"), "DF6601", fmt.Sprintf("%s supports auth %s", h.Frontend, orList(supported)),
			"http.auth is %s, which http.frontend %s doesn't support", h.Auth, h.Frontend)
	case h.Auth == AuthNone && !h.AllowPublic:
		d.errorf(o.at("auth"), "DF6602",
			"set allow_public: true to confirm that anyone may call the exposed queries, or use auth: iam",
			"http.auth is none, but http.allow_public isn't true")
	case h.Auth == AuthNone:
		d.warnf(o.at("auth"), "DF6603", "use auth: iam unless the queries must be public",
			"http.auth is none: anyone with the URL can call every query annotated -- expose: http")
	case h.Auth == AuthAPIKey:
		d.warnf(o.at("auth"), "DF6607", "use iam, lambda or cognito to control access",
			"http.auth is api_key; AWS advises against API keys for authentication or authorization")
	}
}

func authsFor(f Frontend) []Auth {
	switch f {
	case FrontendFunctionURL:
		return []Auth{AuthIAM, AuthNone}
	case FrontendHTTPAPI:
		return []Auth{AuthIAM, AuthJWT, AuthLambda}
	case FrontendRESTAPI:
		return []Auth{AuthIAM, AuthAPIKey, AuthLambda, AuthCognito}
	}
	return nil
}

func (d *decoder) domain(o object) *Domain {
	if !o.has("domain") {
		return nil
	}
	m, ok := d.object("http.domain", o.vals["domain"], o.pos, "name", "routing")
	if !ok {
		return nil
	}
	dom := &Domain{Routing: RoutingLatency}
	d.require(m, "name", "flux.example.com")
	if s, set := d.str(m, "name", "for example name: flux.example.com"); set {
		if isDomainName(s) {
			dom.Name = s
		} else {
			d.errorf(m.at("name"), "DF6606", "use lowercase labels of letters, digits and -, such as flux.example.com",
				"http.domain.name is %q, which isn't a valid DNS name", s)
		}
	}
	enum(d, m, "routing", &dom.Routing, RoutingLatency)
	return dom
}

// isDomainName reports whether s is a lowercase DNS name of at least two labels.
func isDomainName(s string) bool {
	labels := strings.Split(s, ".")
	if len(s) > maxDomainName || len(labels) < 2 {
		return false
	}
	return !slices.ContainsFunc(labels, func(l string) bool {
		return l == "" || len(l) > maxDomainLabel || l[0] == '-' || l[len(l)-1] == '-' || !consists(l, lower+digits+"-")
	})
}

// storeNames checks the resource names that store and prefix alone imply. Without a known
// runtime.split, whether a store-wide projector exists is unknown, so its name isn't checked.
func (d *decoder) storeNames(c *Config, storePos diag.Pos, splitKnown bool) {
	n := c.Naming()
	base := len(c.Prefix) + len(c.Store)
	for _, r := range n.StoreResources(c.Runtime.Split == SplitStream || !splitKnown) {
		if !r.TooLong() {
			continue
		}
		limit := r.Kind.MaxLen()
		d.errorf(storePos, "DF6110",
			fmt.Sprintf("shorten store or prefix: the %s name leaves them %d characters together", r.Kind, limit-(len(r.Name)-base)),
			"the %s name %s has %d characters; the limit is %d", r.Kind, r.Name, len(r.Name), limit)
	}
	if ns := n.ArchiveDatabase(); c.Archive.Catalog == ArchiveCatalogS3Tables && strings.HasPrefix(ns, "aws") {
		d.errorf(storePos, "DF6111", "change prefix or store, or use archive.catalog: glue",
			"the S3 Tables namespace %s starts with aws, which S3 Tables reserves", ns)
	}
}
