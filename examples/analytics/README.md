<!--
Copyright 2026 The Dynaflux Authors
SPDX-License-Identifier: Apache-2.0
-->

# Example: a two-region analytics store

The store `analytics` runs in `us-east-1` and `us-west-2`, with the prefix `demo-` and the
archive on in both regions. Everything Dynaflux generates for it is committed here, so you
can read the client, the runtime's `main.go` and the CloudFormation templates without
running anything.

| Path | What it is |
|------|------------|
| `dynaflux.yaml` | The store's configuration ([config.md](../../docs/spec/config.md)). |
| `dynaflux/` | The SQL: three streams, five views and the queries on them. `api_calls` deduplicates appends (`DEDUP BY`); the views use `count`, `uniq`, `uniqExact`, `sum`, `avg` and `argMax`; `quota_usage` is a `SYNC` view. |
| `app.go` | Application code: `RecordPageView` appends a page view and waits until the page's stats count it. |
| `dynaflux.lock` | The view versions ([ADR 0002](../../docs/decisions/0002-view-version-lock-file.md)). Generated; commit it. |
| `internal/flux/` | The typed client. Generated. |
| `cmd/dynaflux-runtime/main.go` | The Lambda binary that every function runs. Generated. |
| `infra/dynaflux/` | `global.yaml`, `global.stack-policy.json` and `regional.yaml` ([infra.md](../../docs/spec/infra.md)). Generated. |
| `*_test.go` | Tests of the client and of `app.go` against the in-memory store of `fluxtest`, and, with the `integration` build tag, against DynamoDB Local. |
| `e2e/` | The end-to-end suite, which runs this store on DynamoDB Local or on AWS ([runbooks/e2e.md](../../docs/runbooks/e2e.md)). |

## Test and regenerate

From the repository root:

```sh
go test ./examples/analytics/...   # the in-memory tests
make integration                   # every DynamoDB Local test, these included (needs Docker)
make lambda-build                  # builds the Lambda binary for linux/arm64
make infra-lint                    # lints the templates (needs cfn-lint 1.58.0)
```

After changing the SQL, `dynaflux.yaml` or what `generate` writes, regenerate from this
directory and commit the result. `TestExampleIsGenerated` in `internal/cli` fails while the
committed files differ from what `generate` writes.

```sh
go run ../../cmd/dynaflux generate
```

## Deploy

Use a sandbox account. [infra.md §11](../../docs/spec/infra.md#11-deploying) explains each
step; these are its commands for this store, with `us-east-1` as the home region. Lake
Formation must be at its defaults, and you need the SAM CLI and the AWS CLI.

```sh
cd examples/analytics
export CGO_ENABLED=0 GOFLAGS=-tags=lambda.norpc
sam build -t infra/dynaflux/regional.yaml

# 1. The stream tables, in the home region:
sam deploy -t infra/dynaflux/global.yaml --stack-name demo-analytics-global \
  --region us-east-1 --resolve-s3

# 2. First deploy only: forbid replacing the stream tables.
aws cloudformation set-stack-policy --stack-name demo-analytics-global --region us-east-1 \
  --stack-policy-body file://infra/dynaflux/global.stack-policy.json

# 3. The regional resources, in each region:
for region in us-east-1 us-west-2; do
  sam deploy --stack-name demo-analytics-regional --region "$region" \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3
done

# 4. Make every view version live, in both regions:
go run ../../cmd/dynaflux backfill -all -yes
```

- The `Prefix` parameter defaults to `demo-`. For another environment, add
  `--parameter-overrides Prefix=<prefix>` to every deploy, `-prefix <prefix>` to the
  backfill, and use new stack names.
- For alarm notifications, add `AlarmTopicArn=<topic ARN>` to the regional deploys, with a
  topic in that region.
- Until a backfill makes a view version live, queries over it return `ErrViewNotReady`
  ([lifecycle.md §2](../../docs/spec/lifecycle.md#2-deploying-changes)). On this store,
  which has no events yet, step 4 takes about two minutes. Deploy the application after it.

## Use it from an application

Open the store with the prefix it was deployed with, and pass the store to the generated
client:

```go
s, err := dynaflux.Open(dynaflux.Config{AWS: awsCfg, Prefix: "demo-"})
if err != nil {
	return err
}
client := flux.New(s)
```

`awsCfg.Region` is the region the application appends in.

### Read your own writes

In the region of an append, a query sees the append at once only in a `SYNC` view
([sql.md §9.7](../../docs/spec/sql.md#97-mode)). `quota_usage` is one, so a charge can
check the quota it has just used:

```go
_, err := client.AppendAPICall(ctx, call)
usage, err := client.QuotaUsed(ctx, flux.QuotaUsedParams{AccountID: call.AccountID, At: call.EventTime})
```

The other views are `ASYNC`: the projector applies an append about a second later. To
wait for it, append with `dynaflux.Waitable()` and query with `dynaflux.WaitFor`, as
`RecordPageView` in `app.go` does:

```go
tok, err := client.AppendPageView(ctx, view, dynaflux.Waitable())
stats, err := client.PageStats(ctx, params, dynaflux.WaitFor(tok, 3*time.Second))
```

### Permissions

The application's role needs this policy, with your account ID in place of `<account>`
([infra.md §9.9](../../docs/spec/infra.md#99-applications)):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Streams",
      "Effect": "Allow",
      "Action": ["dynamodb:PutItem", "dynamodb:Query"],
      "Resource": "arn:aws:dynamodb:*:<account>:table/demo-analytics-stream-*"
    },
    {
      "Sid": "Views",
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:Query"],
      "Resource": "arn:aws:dynamodb:*:<account>:table/demo-analytics-view-*"
    },
    {
      "Sid": "ViewStates",
      "Effect": "Allow",
      "Action": "dynamodb:Query",
      "Resource": "arn:aws:dynamodb:*:<account>:table/demo-analytics-meta",
      "Condition": {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["view"]}}
    },
    {
      "Sid": "SyncViews",
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "arn:aws:dynamodb:*:<account>:table/demo-analytics-view-quota_usage-v*",
      "Condition": {"ForAnyValue:StringEquals": {"dynamodb:EnclosingOperation": ["TransactWriteItems"]}}
    },
    {
      "Sid": "Receipts",
      "Effect": "Allow",
      "Action": "dynamodb:BatchGetItem",
      "Resource": "arn:aws:dynamodb:*:<account>:table/demo-analytics-meta",
      "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["rcpt#*"]}}
    }
  ]
}
```

- `Streams` appends events and reads them back.
- `Views` reads the view tables. An append to `api_calls` also reads the `quota_usage` row
  it updates.
- `ViewStates` reads the views' states in the meta table. Queries over views and appends to
  `api_calls` check them.
- `SyncViews` lets an append to `api_calls` write the `quota_usage` row, but only in the
  transaction that writes the event.
- `Receipts` lets `WaitFor` read the receipts in the meta table.

The wildcards cover streams and view versions added later: each new version of
`quota_usage` is a new table. They also match the tables of another store whose name
starts with `analytics-stream-` or `analytics-view-`, so don't deploy one next to this
store. Scenarios 34 and 35 of the [end-to-end suite](../../docs/runbooks/e2e.md#8-coded-scenarios)
run as an application with this policy.

## Tear down

Delete the regional stacks, then the global stack:

```sh
for region in us-west-2 us-east-1; do
  sam delete --stack-name demo-analytics-regional --region "$region" --no-prompts
done
sam delete --stack-name demo-analytics-global --region us-east-1 --no-prompts
```

The stacks leave the events and the archive behind
([infra.md §12](../../docs/spec/infra.md#12-changes-after-the-first-deploy)):

- the three stream tables `demo-analytics-stream-*`, a replica in each region, each with
  deletion protection;
- in each region, the bucket `demo-analytics-<account>-<region>-an`, which is versioned;
- in each region, the Glue database `demo_analytics` with the archive's tables and views,
  and the compaction and retention optimizers of each table, which the setup function
  created outside the stacks.

To delete them as well:

1. Turn off deletion protection on every replica, then delete the replicas.
2. In each region, delete the optimizers (`aws glue delete-table-optimizer`), then the Glue
   database, which takes its tables with it. AWS doesn't document whether deleting a table
   deletes its optimizers.
3. Empty each bucket, object versions and delete markers included, then delete it. With the
   optimizers gone, nothing writes to it while it empties.
