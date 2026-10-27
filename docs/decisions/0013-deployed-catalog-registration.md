<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# ADR 0013: The setup function registers the deployed catalog from its own binary

- Status: Accepted. Amends [ADR 0008](0008-archive-schema-migration.md), whose decision
  stands: the catalog's handler reads the binary's catalog, and its `Delete` writes.
- Date: 2026-10-26

## Context

The commands of M5 (`status`, `diff`, `inspect`, `query`, `import`, `backfill`) and the
backfill's first guard need the catalog that a region's functions run, not the local SQL
([lifecycle.md §9](../spec/lifecycle.md#9-the-deployed-catalog),
[ADR 0012](0012-view-version-activation.md)). ADR 0012 keeps it in the region's meta table:
one body item per catalog and a `catalog/current` item that names the current one
([encoding.md §8.4](../spec/encoding.md#84-deployed-catalog)). Something must write those
items at deploy time, after every function runs the new code, and write the previous
catalog again when a deploy rolls back.

ADR 0012 also lets a deploy finish what `dynaflux retire` does: each region's meta table gets
a `retired` tombstone for every version that the deployed catalog lists as retired, so an
environment where `retire` didn't run is complete once deployed. A build that lists a
version as retired stops applying it, so the region must hold the version's tombstone before
that build skips any of its events, whether or not the update succeeds.

What CloudFormation does with a custom resource
([requests][cfn-cr], [responses][cfn-resp], [Update][cfn-update]):

- It sends `Update` only when the resource's properties change.
- When the reply to `Update` carries a new physical ID, the update is a replacement:
  CloudFormation sends `Delete` for the old physical ID in the cleanup phase, after the
  whole stack update succeeded.
- On a rollback, it sends `Update` with the old properties. The documentation doesn't say
  when, in a rolled-back replacement, it deletes which physical ID.

Sizes: the example's catalog is 10.7 KB of JSON, 2.0 KB compressed. A store at the
500-resource template limit holds about 1 MB of JSON. A template holds at most 1 MB.

[ADR 0008](0008-archive-schema-migration.md) says the setup function's handlers work from
the request's properties alone, and rejected a check of the binary's catalog in the
handler because it was the only part of that design that could fail a rollback.

## Decision

**`regional.yaml` has `Catalog`, a `Custom::DynafluxCatalog` custom resource that the setup
function serves with the catalog embedded in its own binary.**

- **Properties:** `MetaTable` (`!Ref Meta`); `CatalogHash`, the lowercase hex SHA-256 of
  the catalog's bytes, which `generate` computes from the same bytes it embeds in the
  binary; and `Generator`, the Dynaflux version that ran `generate`, as in the templates'
  first line. `ServiceTimeout` is 300.
- **Order:** `Catalog` depends on every function: the setup function through its
  `ServiceToken`, the others through `DependsOn`, and through `DependsOn` too on the
  managed policies of their roles, which attach to a role rather than to a function.
  CloudFormation runs it after their code, configuration and grants were updated, so
  `current` names a catalog only once the region runs it and a backfill started then can
  write the new tables.
- **Create and Update:** when `CatalogHash` is the hash of the binary's catalog, the
  handler stores the catalog's body, once per hash, and makes it current. Otherwise it makes
  current the catalog stored under `CatalogHash`, which a rollback asks for: that catalog
  was stored when it was first deployed, whatever code the setup function runs now. A hash
  that was never stored fails the deploy: "deploy the templates and the binary from the
  same dynaflux generate run". `current` gets the request's `Generator`, so a rollback
  restores it too.
- **Physical ID** `<meta table>/<CatalogHash>`. A new catalog is a replacement, and
  CloudFormation deletes the previous registration only after the whole update succeeded.
- **Retired versions:** a `Delete` of a registration that isn't the current one comes from
  that cleanup phase. It sets `retired` each version that the current catalog lists as
  retired and the meta table doesn't hold as `retired`. A `Create` does the same. After a
  rolled-back update, the current catalog is the old one, whose list was applied when it
  was deployed, so nothing new is retired. A `Delete` of the current registration, which
  deleting the stack or rolling back a `Create` sends, retires nothing, and a missing meta
  table is success.
- **Tombstones from the projector:** the handler writes its tombstones after the update,
  but the new build's projectors skip a retired version as soon as they run. So a projector
  whose catalog lists a version as retired, when it finds the version `activating` or `live`
  in a batch of its stream, sets it `retired` before the batch moves past any of its events;
  a batch whose tombstone write fails is retried. A rolled-back update or a failed cleanup
  `Delete` then leaves no version `live` over a table that misses what the new build
  skipped. Such a tombstone is never undone. The handler's tombstones cover the versions
  that no projector met.
- **The stream of a tombstone:** a state item names its version's stream. A version
  without a state item, which never became active in the region, takes its stream from the
  catalog of the registration being deleted, which defined it before it was retired. A
  version that catalog doesn't define, a catalog this binary can't read, and a `Create`,
  which deletes no registration, give it no tombstone.
- **Permissions:** the setup role gains `dynamodb:GetItem`, `PutItem` and `Query` on the
  meta table. The handler retries `AccessDenied` for about 60 seconds, as the other
  handlers do, since the same deploy can add the grant.
- **The binary's bytes:** the runtime passes the handler `catalog.Marshal` of its
  catalog. `catalog.Unmarshal` accepts only the canonical bytes, so these are the bytes
  that `generate` embedded and hashed.

### Rejected

- **The catalog as a property:** an `Update` carries the old and the new properties, so a
  large catalog would travel twice in every update request and grow the template by its
  size, against the template's 1 MB limit.
- **A constant physical ID:** the handler would have to retire on `Update`, before the
  update is known to succeed, and a rolled-back update would retire versions that no build
  skipped. The cleanup
  `Delete` of a replacement is the only signal of success that CloudFormation sends to a
  custom resource.
- **Retiring on every `Delete`:** deleting the current registration happens when the stack
  is deleted or a `Create` rolls back, and retiring then acts for a deploy that didn't
  succeed.
- **The generator from the binary's build information:** during a rollback the setup
  function may still run the newer code, which would label the old catalog with the newer
  version.
- **The projector at cold start, or a CLI step after the deploy:** a store without traffic
  never registers, nothing orders a cold start with the deploy, and a CLI step can be
  forgotten and would make Dynaflux deploy.

## Consequences

- ADR 0008's rule that the handlers work from the request's properties alone holds for the
  stream lookups and the archive schema. The catalog's handler also reads the binary, so a
  template and a binary from different `generate` runs fail the deploy at `Catalog`, unless
  the template names a catalog registered before. A rollback names such a catalog, so the
  check ADR 0008 rejected for the archive schema can't fail a rollback here.
- Each new catalog adds a body item of a few KB to each region's meta table, which never
  expires.
- A Dynaflux upgrade changes `Generator`, so it updates `Catalog` in place: same physical
  ID, no cleanup, nothing retired.
- A rollback doesn't undo a retire: in a region where the new build's projectors handled a
  batch of the version's stream, the version stays `retired`, and queries of it from the
  older build fail with `ErrViewNotReady`. A deploy retires a version without the checks of
  `dynaflux retire`, so each environment runs `retire` before its templates retire the
  version, and `diff` reports one that didn't (DF7115).
- A region created after a version was retired never held it and gets no tombstone for it.
  The version holds no rows there, projectors neither apply nor wait for it (the catalog
  lists it retired), and queries of it fail with `ErrViewNotReady`. Only a backfill run
  under an older build deployed there later could fill it.
- **Unverified, for the AWS end-to-end suite:** that a rollback sends `Update` with the
  old properties to `Catalog`, and when it deletes the new and the old physical IDs; and
  that a failed `Delete` in the cleanup phase doesn't fail the stack.

[cfn-cr]: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-cloudformation-customresource.html
[cfn-resp]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/crpg-ref.html#crpg-ref-responses
[cfn-update]: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/crpg-ref-requesttypes-update.html
