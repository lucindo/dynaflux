// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"cmp"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Versions of the catalog's JSON form. Every change to the form bumps FormatVersion, so an
// older Dynaflux refuses a newer catalog rather than misreading it. [Unmarshal] reads every
// version from MinFormatVersion to FormatVersion, so a new CLI can read the catalogs that
// older releases deployed. A change that only adds omitzero fields keeps MinFormatVersion;
// any other change raises it to the new FormatVersion. That strands the catalogs that older
// generated code embeds, so it also replaces dynaflux.GeneratedCodeVersion1 with the next
// version, and that code fails to compile instead of failing when it starts.
const (
	FormatVersion    = 1
	MinFormatVersion = 1
)

// These types encode as JSON strings.
var (
	_ encoding.TextMarshaler   = Type{}
	_ encoding.TextUnmarshaler = (*Type)(nil)
	_ encoding.TextMarshaler   = Interval{}
	_ encoding.TextUnmarshaler = (*Interval)(nil)
	_ encoding.TextMarshaler   = Retention{}
	_ encoding.TextUnmarshaler = (*Retention)(nil)
)

// envelope keeps the format version first in the JSON and out of [Catalog].
type envelope struct {
	Format  int      `json:"format"`
	Catalog *Catalog `json:"catalog"`
}

// Marshal returns the canonical JSON form of c: compact, with the lists whose order
// carries no meaning sorted, so equal catalogs give equal bytes. It fails for a catalog
// that [Unmarshal] would reject, so a bad catalog fails when it's generated, not when the
// runtime loads it.
func Marshal(c *Catalog) ([]byte, error) {
	if c == nil {
		return nil, errors.New("marshal catalog: catalog is nil")
	}
	data, err := encode(FormatVersion, c)
	if err != nil {
		return nil, fmt.Errorf("marshal catalog: %w", err)
	}
	if _, err = Unmarshal(data); err != nil {
		return nil, fmt.Errorf("marshal catalog: %w", err)
	}
	return data, nil
}

// Unmarshal decodes a catalog that [Marshal] wrote, byte for byte: it rejects unsupported
// format versions, unknown fields and enum values, missing fields, and any other bytes for
// the same catalog. Consumers can rely on the invariants it checks:
//   - the names that SQL defines match [a-z][a-z0-9_]*, and those that become DynamoDB
//     attributes (stream columns, view keys and aggregates) aren't pk or sk
//   - names are unique: streams and views, which share one namespace; queries; a
//     stream's columns, implicit ones included; a view version's keys and aggregates; a
//     query's parameters, output columns and GROUP BY keys
//   - PruneWindowSeconds, Shards, Split and PartitionKeyLen are in range, and storage
//     formats are supported
//   - a view has versions, their numbers are unique and at least 1, at most one is
//     current, and each version's Fingerprint matches its definition
//   - a query's key range has a bound, and its OrderBy is empty only for an aggregation
//     without GROUP BY
//   - every Expr has the fields and argument count its Op takes, and every aggregate the
//     arguments and parameters its function takes
//   - a Nullable column has no Default
//
// Name resolution, types, literal values and the names that SQL doesn't define (the
// store, regions and Go names) are the compiler's to check.
func Unmarshal(data []byte) (*Catalog, error) {
	// The probe reads the version before the strict decode, so a newer catalog gets a
	// version error rather than an unknown-field error. It also rejects trailing data.
	var probe struct {
		Format int `json:"format"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	if probe.Format < MinFormatVersion || probe.Format > FormatVersion {
		return nil, fmt.Errorf("decode catalog: format %d is not supported; this Dynaflux reads formats %d to %d",
			probe.Format, MinFormatVersion, FormatVersion)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var env envelope
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	if env.Catalog == nil {
		return nil, errors.New("decode catalog: no catalog")
	}
	// Re-encoding catches what encoding/json accepts silently: missing or null fields,
	// duplicate keys, keys in another case, and other spellings of the same values.
	canon, err := encode(probe.Format, env.Catalog)
	if err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	if !bytes.Equal(canon, data) {
		return nil, errors.New("decode catalog: not in canonical form")
	}
	return env.Catalog, nil
}

// encode checks c and returns its canonical JSON in the given format version.
func encode(format int, c *Catalog) ([]byte, error) {
	if err := check(c); err != nil {
		return nil, err
	}
	return marshalJSON(envelope{Format: format, Catalog: canonical(c)})
}

// marshalJSON is [json.Marshal] without HTML escaping, so the canonical form escapes only
// what JSON requires (and U+2028 and U+2029, which encoding/json always escapes).
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// canonical returns a copy of c with the unordered lists sorted. It leaves c unchanged.
func canonical(c *Catalog) *Catalog {
	out := *c
	out.Regions = slices.Sorted(slices.Values(c.Regions))
	out.Streams = slices.Clone(c.Streams)
	for i, s := range out.Streams {
		if s.Archive != nil {
			a := *s.Archive
			a.Regions = slices.Sorted(slices.Values(a.Regions))
			out.Streams[i].Archive = &a
		}
	}
	slices.SortStableFunc(out.Streams, func(a, b Stream) int { return strings.Compare(a.Name, b.Name) })
	out.Views = slices.Clone(c.Views)
	for i, v := range out.Views {
		versions := slices.Clone(v.Versions)
		for j, vv := range versions {
			versions[j].Aggregates = sortedAggregates(vv.Aggregates)
		}
		slices.SortStableFunc(versions, func(a, b ViewVersion) int { return cmp.Compare(a.Number, b.Number) })
		out.Views[i].Versions = versions
	}
	slices.SortStableFunc(out.Views, func(a, b View) int { return strings.Compare(a.Name, b.Name) })
	out.Queries = slices.Clone(c.Queries)
	slices.SortStableFunc(out.Queries, func(a, b Query) int { return strings.Compare(a.Name, b.Name) })
	return &out
}

func sortedAggregates(aggs []ViewAggregate) []ViewAggregate {
	out := slices.Clone(aggs)
	slices.SortStableFunc(out, func(a, b ViewAggregate) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// enum is a string enum whose UnmarshalText rejects values it doesn't define.
type enum interface {
	~string
	known() bool
}

func decodeEnum[E enum](dst *E, text []byte, what string) error {
	v := E(text)
	if !v.known() {
		return fmt.Errorf("unknown %s %q", what, text)
	}
	*dst = v
	return nil
}
