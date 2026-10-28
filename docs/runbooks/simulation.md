<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Simulation harness

`internal/simulation` runs the real projector, engine and in-memory store in three regions
with faults, in one `testing/synctest` bubble per seed. A seed replays exactly, so a failure
is reproduced by rerunning its seed. The package doc lists the properties a run checks.

## Tiers

| Target | What runs | Time |
|--------|-----------|------|
| `make test` | The smoke tier with `-race`: one seed of each scenario but `poison`, `long` and `gaps`, two seeds of `late` and `sync` run twice, control C3 at 2 seeds, and the scripted tests | 25 s for `internal/simulation` |
| `make sim` | The full tier: 300 seeds × 3 regions of each scenario, 100 of `guards-table`, `rollback`, `retire`, `timesplit-late`, `table`, `sync-backfill`, `sync-regions`, `wait` and `gaps`, and four seeds each of `late`, `sync` and `regions` run twice | 34 min |
| `make sim-controls` | Every negative control: code mutants, environment and configuration controls, baselines and self-checks. Each must be caught | 28 min |
| `make sim-seed SCENARIO=x SEED=n` | One seed of one scenario, verbose | 1–3 s |

`make sim` and `make sim-seed` leave `-race` off; set `SIM_RACE=-race` to turn it on. The
scheduler runs one actor at a time, so `-race` finds only races between goroutines the core
starts, and the smoke tier already covers those.

The times are from a 12-core Apple silicon laptop in October 2026. Every scenario with a
backfill runs its jobs through `backfill.Step`, which stages and merges the archive or the
stream table, so those seeds cost the most. The three `sync` scenarios run the store of
`testdata/sync`, whose views are `SYNC`, and take about 30 seconds of the full tier. `slices`
samples CloudWatch's `DataFreshness` for its watermark and takes about 90 seconds.

## A failing seed

A failure names its rerun command, such as `make sim-seed SCENARIO=faults SEED=17`. The seed
runs the same in either tier. A run of one seed skips the check that the tier made every
fault happen.

## Negative controls

`go run ./internal/simulation/mutants` runs the table in
`internal/simulation/mutants/table.go`. A code mutant replaces one piece of source text
through `go test -overlay`, so no file changes; the run fails when the text no longer occurs
exactly once. The runner prints, for each control, the share of seeds that caught it, and
flags a control caught in under 10% of seeds as weak. `-run` picks controls by ID:

```sh
go run ./internal/simulation/mutants -run '^C1[0-3]$'
```

A control with a scripted test is caught when that test fails. Where its scenario catches a
control in too few seeds, a scripted test of the same rule backs it instead:

| Control | Scripted test | Why not its scenario |
|---------|---------------|----------------------|
| C53 | `TestStepFencesEveryVersion` in `internal/backfill` | `guards` caught it in 13 of 300 seeds: a batch of an older build must read its unit before the activation and move its progress after the cut |
| C73 | `TestStepNoSplit` in `internal/backfill` | `guards-table` caught it in about 7 of 100 seeds: the batch of an older build must take the only record of a row |
| C104 | `TestWaitReadsTheStateAgain` in `internal/engine` | `wait` caught it in none of 100 seeds: a waitable event must break a live version, which the backfills of the poisoned versions seldom leave, and its receipt land within 30 seconds of the reader's cached state read |
| C106 | `TestHandleFailsTheRecordOfAFailedReceipt` in `internal/projector` | `wait` caught it in none of 100 seeds: two receipt writes of one batch must fail |
| C109 | `TestHandleRecordsALagBeforeTheViewStates` in `internal/projector` | no scenario trims records: a projector that fails on the view states for a day hands the old records to the batch after it, which records the lag |
| C110 | `TestArchiveFacts` in `internal/backfill/awsbackfill` | the harness's witness shows no CloudWatch data |
| C93 | `TestDelivered` in `internal/backfill` | `slices` caught it in none of 100 seeds: the clock margin covers a period's 60 seconds, so only a CloudWatch clock ahead of the job's, by more than the datapoint's place in its period, passes a record put before the cut, and only for seconds |
