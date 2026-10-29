<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Changelog

This file lists the changes of each Dynaflux release, newest first, and what you must do
when you upgrade. Its format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The first release. It needs Go 1.26 or later.

### Added

- A SQL dialect, in the style of ClickHouse, for streams of events, materialized views and
  named queries, and `dynaflux.yaml` for a store's name, regions and settings.
- `dynaflux validate`, which rejects any query that DynamoDB can't serve with key lookups
  and bounded range reads, and `dynaflux explain`, which estimates what a design costs.
- `dynaflux generate`, which writes a typed Go client package, the `main.go` of the Lambda
  binary, and the CloudFormation templates, and numbers view versions in `dynaflux.lock`.
- The library: `dynaflux.Open`, appends that `DEDUP BY` makes safe to retry, `SYNC` views,
  wait tokens, queries, and `fluxtest`, an in-memory store for tests.
- The projector, a Lambda function in each region, which keeps views current and archives
  every event to Apache Iceberg tables on S3.
- The operator commands `status`, `diff`, `backfill`, `retire`, `inspect`, `query` and
  `import`, and `init` and `dev` for local development.
- HTTP endpoints for the queries marked `-- expose: http`.
- The compatibility promise of v1.x, in
  [ADR 0019](https://github.com/lucindo/dynaflux/blob/main/docs/decisions/0019-v1-compatibility-promise.md).
