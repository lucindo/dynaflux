<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0010: `runtime.Run` starts every function, and the projector core stops sending before Lambda's deadline

- Status: Accepted. Amends [ADR 0004](0004-runtime-storage-boundary.md), whose decision stands.
- Date: 2026-10-26
- Amended by [ADR 0019](0019-v1-compatibility-promise.md): the package is `fluxruntime`, and
  later options on `Run` come as a new function, since a variadic parameter changes `Run`'s
  type

## Context

M3 adds the AWS runtime: one Lambda binary per store, which `dynaflux generate` writes as
`cmd/dynaflux-runtime/main.go`. Every function of the store runs that binary, and the
variable `DYNAFLUX_MODE` names the one it serves: `projector` (M3), `setup` (right after
M3), `backfill` (M5) and `query` (M6) ([infra.md](../spec/infra.md)).
[ADR 0006](0006-generated-code-contract.md) kept the package name
`github.com/lucindo/dynaflux/runtime` and left its entry point to M3. The plan had sketched
`runtime.Start(flux.Catalog, flux.Handlers())`, with custom handlers that no milestone
schedules.

The projector prunes high-water marks ([encoding.md §7.6](../spec/encoding.md#76-high-water-marks)).
Pruning is exact only if the runtime keeps two promises:

1. **No write lands after its invocation's deadline.** When a handler returns, Lambda
   freezes the goroutines still running and resumes them in a later invocation, possibly
   minutes later ([execution environment][lifecycle]). A write resumed that way could land
   after another invocation pruned the mark it relied on, and count events twice.
2. **`progress_at` is never earlier than the moment its write lands.** The conditional write
   that raises a delivery unit's `progress` also sets `progress_at`
   ([encoding.md §8.1](../spec/encoding.md#81-unit-item)), and pruning relies on it.

An entry is pruned only once its unit's `progress_at` is more than
[`runtime.prune_window_seconds`](../spec/config.md#runtime) (W, default 960) in the past,
and W must be at least `timeout_seconds` + 60 (DF6305). The 60 seconds are a margin for a
request that DynamoDB applies late, and for clock skew between hosts.

M2's core takes one deadline from its context: it sends nothing after it, and stamps
`progress_at` with it. If the Lambda adapter shortened that context to keep time for its
reply, the stamp would be the earlier time, and a progress write sent just before it could
land after it, which breaks promise 2.

Two facts about the client side:

- The AWS SDK checks the context before each attempt and during backoff, and `net/http`
  closes the connection when the context ends during a request. Measured with
  aws-sdk-go-v2 v1.47.4: an expired context sends nothing, and a call to a server that
  never answers returns 2.5 ms after the deadline. A context ends on a timer, which can
  fire late, so an attempt started just before the deadline can still reach the server
  about a millisecond after it.
- Closing a TCP connection with Go's default linger lets the operating system keep sending
  bytes it has buffered ([`SetLinger`][linger]), even after the process is frozen or has
  exited. DynamoDB accepts a signed request for about 5 minutes, far longer than the 60
  second margin.

## Decision

**Entry point.** Package `runtime` exports one function:

```go
// Run serves the function that DYNAFLUX_MODE names, for the store of cat. It returns only
// when the function can't start, and the generated main exits with that error.
func Run(cat *dynaflux.Catalog) error
```

The generated `main` calls `runtime.Run(flux.Catalog)` and exits with `log.Fatal` when it
returns an error.

- `Run` checks everything offline, during Lambda's Init phase: the catalog,
  `DYNAFLUX_STORE` (it must equal the catalog's store), `DYNAFLUX_PREFIX`, `AWS_REGION`, and
  `AWS_LAMBDA_RUNTIME_API` (the binary runs only on Lambda). An error exits the process, so
  Lambda logs `INIT_REPORT … Status: error` and retries Init on the next invocation.
- Modes `projector` and `setup` start their handler with aws-lambda-go's `lambda.Start`.
  `query` and `backfill` return an error that says they need a later Dynaflux release. Any
  other value is an error.
- In projector mode, `Run` refuses `AWS_LAMBDA_MAX_CONCURRENCY` above 1. That variable
  means Lambda Managed Instances, which run several invocations in one process, and AWS
  doesn't document whether they freeze like on-demand Lambda, which promise 1 relies on.

**Deadline.** The projector adapter calls `Core.Handle` synchronously, with Lambda's
context unchanged. The core:

- stops sending at the cutoff: the deadline less a reserve of min(2 s, remaining / 10),
  where remaining is the time left when `Handle` starts. That is 2 s at the 60 s default
  timeout and at 900 s, and 0.5 s at 5 s. The reserve leaves time to reply with the rows
  written so far, so Lambda checkpoints them instead of timing out and retrying the whole
  batch;
- stops the archive and the rows half a reserve before the cutoff, so a row write that never
  answers still leaves the progress write time to checkpoint the rows written below it;
- stamps `progress_at` with the deadline itself, never with the cutoff;
- fails the batch with `ErrLifetime` when the deadline is more than W − 60 s away;
- waits for every row write before it returns, on success and on error.

The adapter never returns while `Handle` runs (so no timeout wrapper that returns on
`ctx.Done()`), starts no background work, and never retries: Lambda redelivers.

**Connections.** The runtime's HTTP client dials with `SO_LINGER 0`. Closing a connection
then sends a reset and drops the bytes not yet sent.

**Assumption.** What remains is an assumption about DynamoDB: a request it received in full
before the deadline is applied within 60 seconds. AWS doesn't document it, and the margin
in W is there for it.

### Rejected

- **`runtime.Start(cat, handlers)`, or `Run` with options now:** no milestone schedules
  custom handlers. Adding `...Option` to `Run` later keeps the one generated call compiling.
- **The package name `fluxruntime`:** ADR 0006 kept `runtime`.
- **The cutoff in the adapter,** with a shortened context for the core: the core would
  stamp `progress_at` with the cutoff, which breaks promise 2. Passing the real deadline in
  the batch next to a shortened context would give the core two deadlines to keep apart.
- **Go's default linger:** buffered bytes of a cancelled request can still reach DynamoDB
  after the deadline.
- **A dialer of our own that sets the write deadline:** the runtime would wrap the
  connections it dials so that a cancelled request's connection already has a past write
  deadline when `net/http` closes it. The `close_notify` alert would then fail at once
  instead of blocking up to 5 s (see the `close_notify` edge below). Correctness doesn't
  need it, so it isn't built. It is the alternative to build if the maintainer's end-to-end
  runs show that the timed-out invocations matter.

## Consequences

- Generated code makes one call, and options can be added later without breaking it.
- Pruning waits up to 2 s longer, since `progress_at` is the deadline and not the cutoff.
- The simulation harness runs the same core, so its invocations stop at the same cutoff,
  and it gains a negative control that sends past the cutoff.
- `TestNoRequestAfterDeadline` reruns on every SDK upgrade, over TLS through the
  `SO_LINGER 0` dialer, against a server that stops reading in the middle of a request. If
  closing a TLS connection takes longer than the reserve in any case but the `close_notify`
  edge below, the reserve comes back for a decision.
- In the `close_notify` edge, an invocation can time out and its batch is retried.
  Correctness holds.
- Only AWS can confirm the 60-second assumption and how the clocks behave across a freeze.
  Both are checks of the maintainer's end-to-end runs.

### Measured TLS close time

Measured in M3 with aws-sdk-go-v2 v1.47.4 and Go 1.26.9 over loopback TLS, on macOS and on
Linux in Docker:

- In `TestNoRequestAfterDeadline`'s cases (throttled, hung, expired, and a server that stops
  reading mid-request), `Put` returns 0.2–2.7 ms after the deadline, TLS close included.
- Over about 1,000 cancelled requests of 1 KB to 400 KB to a server that stopped reading,
  `Put` returned at most 11 ms after the deadline on Linux and 3.4 ms on macOS.
- With `SO_LINGER 0` the stalled server never received the request whole. With the default
  linger it received it every time, after the client had returned.
- One case can exceed the reserve: the `close_notify` edge. When a request is already in
  the kernel, written whole into a send buffer that is then full, and the server reads
  nothing, `crypto/tls` first writes a `close_notify` alert under a 5 s write deadline,
  and only then closes the socket. On Linux, where Lambda runs, closing that connection
  can block about 5 s past the cutoff, and until it returns the kernel can still send the
  buffered request if the server starts reading again. Built directly, with a TLS
  connection whose send buffer was filled, `Close` took 5.0 s on Linux and on macOS. The
  runs through `net/http` never hit it.
- Correctness holds in that edge. The late write lands at most about 5 s past the cutoff,
  well inside the 60-second margin of W, and `progress_at` is still the deadline. The only
  cost is the invocation: the handler is still running at the deadline, so Lambda times it
  out, resets the environment and delivers the batch again. A dialer that sets the write
  deadline (under Rejected) removes the block, if end-to-end runs show it matters.
- The projector doesn't run on Lambda Managed Instances until their freeze and timeout
  behavior is verified.

[lifecycle]: https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html
[linger]: https://pkg.go.dev/net#TCPConn.SetLinger
