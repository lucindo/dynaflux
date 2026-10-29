// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

// Attribute names of Dynaflux items. An event's payload is exactly the attributes named
// after its stream's declared columns, whose names start with a letter and aren't pk or
// sk. Every other attribute is bookkeeping, so code that compares payloads selects the
// declared columns rather than leaving these out.
const (
	AttrPK  = "pk"   // Partition key of every table.
	AttrSK  = "sk"   // Sort key of every table.
	AttrTTL = "_ttl" // Expiry in epoch seconds, in every table that expires items.

	AttrOriginRegion = "_r"    // Event: the region that accepted it.
	AttrIngestTime   = "_it"   // Event: ingest_time in microseconds.
	AttrEventVersion = "_ver"  // Event: reserved for consistent streams.
	AttrSync         = "_sync" // Event: the SYNC view versions its append's transaction applied.
	AttrReceipt      = "_rcpt" // Event: its append asked for a receipt (dynaflux.Waitable).

	AttrRowVersion     = "_v"   // View row: version for the conditional read-modify-write.
	AttrHighWaterMarks = "_hwm" // View row: last applied sequence number per delivery unit.
	AttrMarkerOwner    = "_o"   // uniqExact marker: the record that introduced the value.
)

// KeyAttrType is the DynamoDB type of [AttrPK] and [AttrSK] in every table of storage
// format 1: a string in an order-preserving encoding. A storage format with another key
// type turns this into a function of the format.
const KeyAttrType = "S"

// SequenceWidth is the width of a stored change-stream sequence number. They have up to 40
// digits, more than a DynamoDB number holds, so they're stored as zero-padded strings,
// which sort in numeric order.
const SequenceWidth = 40

// Environment variables of the runtime functions.
const (
	EnvStore  = "DYNAFLUX_STORE"
	EnvPrefix = "DYNAFLUX_PREFIX"
	EnvMode   = "DYNAFLUX_MODE"
	// EnvArchiveSchema is the archive schema's fingerprint, which projectors get in the
	// regions that archive (ADR 0008).
	EnvArchiveSchema = "DYNAFLUX_ARCHIVE_SCHEMA"
	// EnvCatalogHash is the hash of the catalog that generated the query function's
	// template, which the function checks against its binary's at Init (ADR 0018).
	EnvCatalogHash = "DYNAFLUX_CATALOG_HASH"
)

// Values of [EnvMode]: the function that the runtime binary serves.
const (
	EnvModeProjector = "projector"
	EnvModeSetup     = "setup"
	EnvModeQuery     = "query"
	EnvModeBackfill  = "backfill"
)

// Runtime metrics, which the functions publish in CloudWatch embedded metric format. Each
// has the dimension [MetricDimensionStore], whose value is {prefix}{store}, so that two
// environments in one account and region don't share alarms.
const (
	MetricNamespace      = "Dynaflux"
	MetricDimensionStore = "Store"
	// MetricBreakerOpened counts view versions whose breaker opened.
	MetricBreakerOpened = "BreakerOpened"
	// MetricRowTooLarge counts view row writes refused for passing DynamoDB's item size limit.
	MetricRowTooLarge = "RowTooLarge"
	// MetricDivergentDuplicates counts events written twice under one dedup key with
	// different payloads.
	MetricDivergentDuplicates = "DivergentDuplicates"
	// MetricLeftOut counts the view source rows and aggregate inputs left out for a value
	// too long to store (sql.md §5.8).
	MetricLeftOut = "LeftOut"
	// MetricQueryFailures counts the query function's answers with status 500 or 504.
	MetricQueryFailures = "QueryFailures"
)
