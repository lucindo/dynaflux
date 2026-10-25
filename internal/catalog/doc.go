// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package catalog is the compiled model of a Dynaflux store: its streams, every live
// version of its views, and its named queries, with expressions as a typed IR.
//
// The catalog is the contract between the compiler, which builds it, and code generation,
// the runtime and the infrastructure renderer, which consume it. Generated code can't
// import this package, so it embeds the catalog as JSON bytes ([Marshal]) that the runtime
// decodes ([Unmarshal]). The JSON form is versioned ([FormatVersion]) and decoded strictly.
//
// The catalog holds logical names only. [Naming] derives the AWS resource names from a
// prefix, which belongs to the deployment environment, not to the catalog.
package catalog
