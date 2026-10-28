// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package infra builds the infrastructure manifest: the description of one store's AWS
// resources that dynaflux generate derives from dynaflux.yaml and the catalog, and that a
// renderer such as package cloudformation turns into templates. docs/spec/infra.md
// describes what users deploy; this package is internal and nothing reads a manifest back.
//
// The manifest makes every protocol and safety decision: names, capacity, retention,
// deletion and replacement. A renderer owns only its tool's mechanics, so a second renderer
// can't drift from the first on anything that affects data. [Build] reads only the config
// and the catalog, never AWS, and the same inputs give the same manifest: resources are
// sorted by ID, regions are sorted, and nothing depends on time or on the Dynaflux release.
//
// Resources are in three scopes. Global holds what is deployed once for the store, in one
// region of the store: the stream Global Tables. Regional holds what is deployed in every
// region. Query, deployed in every region with the application, holds the query function
// and its front door when a query is annotated -- expose: http.
//
// Every resource has an ID, unique across the scopes: its logical identifier in a
// template, which never changes for the life of a store. It is a kind word, such as
// "Stream", followed by a part derived from the stream or view name alone. No kind word is
// the start of another, so IDs of different kinds can't collide. Fields that name or point
// at an AWS resource are [Text] values, which hold the deploy-time placeholders and the
// references between resources.
package infra
