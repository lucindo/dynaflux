// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package keys implements the storage encoding of docs/spec/encoding.md: item keys and
// sort-key ranges, hashes and identifiers, attribute values, and high-water marks.
//
// Keys are encoded and never decoded: every item stores its key columns as attributes. The
// one exception is a stream item's sort key, whose two fields have fixed widths.
package keys
