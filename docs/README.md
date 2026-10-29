<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux documentation

Start with the [README](../README.md), then [getting started](guide/getting-started.md).

## Guides

The guides show how to build, run and pay for a store.

| Document | What it covers |
|----------|----------------|
| [guide/getting-started.md](guide/getting-started.md) | Build a store of page views, run it on your machine against DynamoDB Local, deploy it to two AWS regions, use it, and delete it. Every command the path needs is on the page. |
| [guide/concepts.md](guide/concepts.md) | The parts of a store, how an event flows through them, what to expect across regions and when a region fails, and what Dynaflux doesn't do. |
| [guide/application.md](guide/application.md) | The Go code that uses a store: the generated package, opening a store, appending, reading streams, queries, the Go types of SQL values, errors, tests and production settings. |
| [guide/dialect.md](guide/dialect.md) | Recipes for streams, views and queries, by task, with why each one works and the most common diagnostics and their fixes. |
| [guide/operations.md](guide/operations.md) | Running a deployed store: environments, reading `status`, change procedures, backfills, alarms, broken versions, debugging, the archive in Athena and teardown. |
| [guide/cost.md](guide/cost.md) | Where a store's money goes, why the defaults are what they are, the settings that lower the bill, and how to read `dynaflux explain`. |

## Specifications

The specifications are normative: they define what Dynaflux accepts and what it rejects.

| Document | What it covers |
|----------|----------------|
| [spec/sql.md](spec/sql.md) | The SQL dialect: types, expressions, functions, streams, materialized views and named queries. [Section 12](spec/sql.md#12-worked-examples) has worked examples, and [Appendix A](spec/sql.md#appendix-a-diagnostics) lists every diagnostic code with its message and hint. |
| [spec/config.md](spec/config.md) | The `dynaflux.yaml` schema: every key and its default, the SQL file patterns, the AWS resource names, and the diagnostic codes for the file. |
| [spec/encoding.md](spec/encoding.md) | How Dynaflux stores data in DynamoDB: item keys, attribute values, aggregate states, and the bookkeeping items in the meta table. |
| [spec/infra.md](spec/infra.md) | The CloudFormation templates that `dynaflux generate` writes: every resource with its name and settings, IAM, alarms, and how to deploy and change them. |
| [spec/lifecycle.md](spec/lifecycle.md) | What happens to a view version from deploy to removal: its states, activation, backfill, when queries may read it, repairs and retiring, and the diagnostics of the operator commands. |
| [spec/http.md](spec/http.md) | The wire format of the query function: routes, request and response bodies, value forms, error codes, health, limits, what each kind of authentication asks of a caller, and a Python client. |

## Decisions

[decisions/](decisions/README.md) holds the architecture decision records. Each one records a
decision that shapes Dynaflux: the problem, what we chose and rejected, and what follows from
it.

## Runbooks

| Document | What it covers |
|----------|----------------|
| [runbooks/simulation.md](runbooks/simulation.md) | The simulation harness: its test tiers and how long they take, rerunning a failing seed, and the negative controls. |
| [runbooks/e2e.md](runbooks/e2e.md) | The end-to-end suite on DynamoDB Local, and the maintainer's procedure for it on AWS, with the one-time experiments whose answers go into the specifications. |
| [runbooks/benchmarks.md](runbooks/benchmarks.md) | The Go benchmarks: what they measure, running them, and comparing a run with the recorded baseline. The throughput benchmark: checking it on DynamoDB Local with `make bench-emulator`, and the maintainer's run on AWS with `make bench-aws`, its report and its cost. |
