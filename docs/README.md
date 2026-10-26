<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Dynaflux documentation

User guides, such as getting started, operations and cost, come later. Until then, start
with the [README](../README.md).

## Specifications

The specifications are normative: they define what Dynaflux accepts and what it rejects.

| Document | What it covers |
|----------|----------------|
| [spec/sql.md](spec/sql.md) | The SQL dialect: types, expressions, functions, streams, materialized views and named queries. [Section 12](spec/sql.md#12-worked-examples) has worked examples, and [Appendix A](spec/sql.md#appendix-a-diagnostics) lists every diagnostic code with its message and hint. |
| [spec/config.md](spec/config.md) | The `dynaflux.yaml` schema: every key and its default, the SQL file patterns, the AWS resource names, and the diagnostic codes for the file. |
| [spec/encoding.md](spec/encoding.md) | How Dynaflux stores data in DynamoDB: item keys, attribute values, aggregate states, and the bookkeeping items of the projector. |

## Decisions

[decisions/](decisions/README.md) holds the architecture decision records. Each one records a
decision that shapes Dynaflux: the problem, what we chose and rejected, and what follows from
it.

## Runbooks

| Document | What it covers |
|----------|----------------|
| [runbooks/simulation.md](runbooks/simulation.md) | The simulation harness: its test tiers and how long they take, rerunning a failing seed, and the negative controls. |
