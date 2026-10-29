// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package dynaflux runs event-sourced fact streams and materialized views on DynamoDB
// Global Tables. You declare streams, views and queries in SQL, and the dynaflux command
// generates typed Go code and infrastructure from them.
//
// An application opens a [Store], as [Open]'s example shows, and passes it to the generated
// package: here flux, the client of a store with a stream page_views and a query PageStats
// over a view of it:
//
//	client := flux.New(store)
//
// An append with [Waitable] returns a token that a query can wait for, so that the query's
// rows include the event:
//
//	view := flux.PageView{UserID: "u1", Page: "/home", EventTime: time.Now()}
//	tok, err := client.AppendPageView(ctx, view, dynaflux.Waitable())
//	if err != nil {
//		return err
//	}
//	day := flux.PageStatsParams{Page: view.Page, From: view.EventTime, To: view.EventTime}
//	rows, err := client.PageStats(ctx, day, dynaflux.WaitFor(tok, 5*time.Second))
//	if err != nil {
//		return err
//	}
//	for _, row := range rows {
//		fmt.Println(row.Day, row.Views, row.Visitors)
//	}
//
// A stream read is a range loop, which yields at most one error:
//
//	key := flux.PageViewsKey{UserID: "u1"}
//	for ev, err := range client.ReadPageViews(ctx, key, dynaflux.Since(time.Now().Add(-time.Hour))) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(ev.Page, ev.EventTime)
//	}
//
// Errors wrap the sentinels below, for [errors.Is], and the types [ValueError] and
// [BatchError], for [errors.AsType]:
//
//	switch ve, ok := errors.AsType[*dynaflux.ValueError](err); {
//	case ok:
//		fmt.Println("Dynaflux can't store the value of", ve.Name) // a column or query parameter
//	case errors.Is(err, dynaflux.ErrDuplicate):
//		// A stream with DEDUP BY stored the event before; tok names that event.
//	case errors.Is(err, dynaflux.ErrWaitTimeout):
//		// The projector hasn't handled the event yet: query again later.
//	}
//
// The [getting-started guide] builds and deploys such an application.
//
// [getting-started guide]: https://github.com/lucindo/dynaflux/blob/main/docs/guide/getting-started.md
package dynaflux

// GeneratedCodeVersion1 is the contract between the code that dynaflux generate writes and
// this module. Generated code names the generator's current GeneratedCodeVersionN, whatever
// features the project uses. When new generated code needs something that an older module
// lacks, the generator names the next N, so that code fails to compile at that line against
// the older module, not with "undefined: …". Every v1 release keeps every constant it has
// defined.
const GeneratedCodeVersion1 = 1
