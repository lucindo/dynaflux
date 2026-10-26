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
| `make test` | The smoke tier with `-race`: one seed of each scenario but `poison` and `long`, two seeds of `late` run twice, control C3 at 2 seeds, and the scripted tests | 10 s for `internal/simulation` |
| `make sim` | The full tier: 300 seeds × 3 regions of each of the 8 scenarios, and four seeds each of `late` and `regions` run twice | 6.5 min |
| `make sim-controls` | Every negative control: code mutants, environment and configuration controls, baselines and self-checks. Each must be caught | 5.5 min |
| `make sim-seed SCENARIO=x SEED=n` | One seed of one scenario, verbose | 1–3 s |

`make sim` and `make sim-seed` leave `-race` off; set `SIM_RACE=-race` to turn it on. The
scheduler runs one actor at a time, so `-race` finds only races between goroutines the core
starts, and the smoke tier already covers those.

The times are from a 12-core Apple silicon laptop in October 2026. `long` takes 40% of
`make sim`: a seed of it costs three times one of `d23a`.

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

A control with a scripted test is caught when that test fails.
