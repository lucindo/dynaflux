// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/archive"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
)

// accountIDLen is the length of an AWS account ID.
const accountIDLen = 12

// Options are the one-shot choices of a dynaflux generate run, which dynaflux.yaml doesn't
// hold.
type Options struct {
	// UnprotectRegion, when set, is a region of the store whose stream table replicas get no
	// deletion protection: the first step of removing the region.
	UnprotectRegion string
}

// Build derives the manifest of a store from its checked config and its catalog, and
// checks it. It fails when the catalog has no stream, since there is nothing to deploy,
// and when a role's policies are over IAM's size limit.
func Build(cfg *config.Config, cat *catalog.Catalog, opts Options) (*Manifest, error) {
	if len(cat.Streams) == 0 {
		return nil, errors.New("infra: the store has no stream, so there is nothing to deploy")
	}
	archiveRegions, schema, err := archivePlacement(cat)
	if err != nil {
		return nil, err
	}
	// Both are clean relative paths, which config checks.
	code, err := filepath.Rel(filepath.FromSlash(cfg.Infra.Out), filepath.FromSlash(cfg.Runtime.Out))
	if err != nil {
		return nil, fmt.Errorf("infra: runtime.out relative to infra.out: %w", err)
	}
	regions := slices.Sorted(slices.Values(cat.Regions))
	longest := slices.MaxFunc(regions, func(x, y string) int { return len(x) - len(y) })
	b := &builder{
		cfg:         cfg,
		cat:         cat,
		opts:        opts,
		naming:      catalog.Naming{Prefix: placeholder(PlaceholderPrefix), Store: cat.Store},
		regions:     regions,
		worst:       worstCase{region: strings.Repeat("r", len(longest))},
		maxLen:      math.MaxInt,
		code:        filepath.ToSlash(code),
		schema:      schema,
		archiveOnly: len(archiveRegions) > 0 && len(archiveRegions) < len(regions),
	}
	m := &Manifest{Store: cat.Store, Regions: regions, Global: b.global(), Regional: b.regional()}
	dashboard, err := b.dashboard(&m.Regional)
	if err != nil {
		return nil, err
	}
	m.Regional.Dashboards = []Dashboard{dashboard}
	m.Prefix = Prefix{Default: cfg.Prefix, MaxLen: b.maxLen}
	if b.archiveOnly {
		m.ArchiveRegions = archiveRegions
	}
	if err = m.Check(); err != nil {
		return nil, err
	}
	if err = m.checkPolicySizes(b.worst, cfg.Runtime.Split == config.SplitStream); err != nil {
		return nil, err
	}
	return m, nil
}

type builder struct {
	cfg  *config.Config
	cat  *catalog.Catalog
	opts Options
	// naming makes the names of the manifest, with ${prefix} as the prefix.
	naming  catalog.Naming
	regions []string
	// worst resolves names to their longest deployed form, without the prefix.
	worst worstCase
	// maxLen is the longest prefix that every name made so far allows.
	maxLen int
	// code is runtime.out relative to infra.out.
	code string
	// schema is the archive of each archive region, with no table when nothing is archived.
	schema archive.Schema
	// archiveOnly is set when only some regions archive, so archive resources and statements
	// are archive-only.
	archiveOnly bool
}

// archives reports whether the store archives a stream.
func (b *builder) archives() bool {
	return len(b.schema.Tables) > 0
}

// archivePlacement returns the regions that archive a stream, sorted, and the archive
// schema they hold. One template serves every region, so every archived stream must have
// the same regions, as validate gives them.
func archivePlacement(cat *catalog.Catalog) ([]string, archive.Schema, error) {
	var first string
	var regions []string
	for _, s := range cat.Streams {
		if s.Archive == nil {
			continue
		}
		rs := slices.Sorted(slices.Values(s.Archive.Regions))
		if first == "" {
			first, regions = s.Name, rs
		} else if !slices.Equal(rs, regions) {
			return nil, archive.Schema{}, fmt.Errorf("infra: streams %s and %s archive in different regions, and one template serves every region", first, s.Name)
		}
	}
	if len(regions) == 0 {
		return nil, archive.Schema{}, nil
	}
	return regions, archive.SchemaFor(cat, regions[0]), nil
}

// name returns a name that naming made for a resource of kind, and lowers maxLen to the
// longest prefix with which it fits the kind's limit in every region and account.
func (b *builder) name(kind catalog.ResourceKind, name string) Text {
	return b.fit(kind.MaxLen(), name)
}

// fit returns name, and lowers maxLen to the longest prefix with which name is at most limit
// characters in every region and account.
func (b *builder) fit(limit int, name string) Text {
	prefixes := strings.Count(name, placeholder(PlaceholderPrefix)) + strings.Count(name, placeholder(PlaceholderPrefixUnderscore))
	// Check reports a name that doesn't resolve.
	if fixed, err := b.worst.text(Text(name)); err == nil && prefixes > 0 {
		b.maxLen = min(b.maxLen, (limit-len(fixed))/prefixes)
	}
	return Text(name)
}

// tags returns the tags of a resource of kind that belongs to the stream or view called
// name, or to the whole store when name is empty.
func (b *builder) tags(kind, name string) map[string]string {
	t := map[string]string{TagStore: b.cat.Store, TagKind: kind}
	if name != "" {
		t[TagName] = name
	}
	return t
}

// tableCapacity returns the capacity of the tables of the stream or view called name: its
// tables entry, or the default.
func (b *builder) tableCapacity(name string) Capacity {
	if i := slices.IndexFunc(b.cfg.Tables, func(t config.Table) bool { return t.Name == name }); i >= 0 {
		return capacity(b.cfg.Tables[i].Capacity)
	}
	return capacity(b.cfg.Defaults.Capacity)
}

func capacity(c config.Capacity) Capacity {
	if c.Mode != config.CapacityProvisioned {
		return Capacity{Mode: CapacityOnDemand}
	}
	return Capacity{Mode: CapacityProvisioned, Read: scaling(c.Autoscaling.Read), Write: scaling(c.Autoscaling.Write)}
}

func scaling(s config.Scaling) *Scaling {
	return &Scaling{Min: s.Min, Max: s.Max, TargetPercent: s.TargetPercent}
}

// tableKeys returns the key attributes of every table (encoding.md §2).
func tableKeys() []Key {
	return []Key{
		{Name: catalog.AttrPK, Type: catalog.KeyAttrType, Key: KeyHash},
		{Name: catalog.AttrSK, Type: catalog.KeyAttrType, Key: KeyRange},
	}
}
