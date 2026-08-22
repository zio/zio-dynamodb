---
id: index
title: "CRUD Operations"
---

## Introduction

Every operation below compiles down to the same `DynamoDBQuery` ADT and runs through the same
interpreter — the Low-Level and High-Level APIs are two different ways to *build* a query,
not two different execution paths. You can mix both in the same program.

- **Low-Level** — `DynamoDBQuery.getItem`/`putItem`/`updateItem`/... — shaped directly after
  the AWS SDK: string table names, `Item`/`PrimaryKey` maps, string-keyed projection
  expressions (`$("fieldName")`). No `Schema` required. See [Low-Level API](low-level.md).
- **High-Level** — `DdbExprApi.get`/`put`/`update`/... — schema-derived: your own case
  classes, `CompanionOptics`-generated `Lens`es instead of string field names, compile-time
  checked condition/key expressions. Requires `derives Schema` on your model. See
  [High-Level API](high-level.md).

**Unlike 2.x, batching and parallelism are always explicit, never automatic.** Query
execution never batches or parallelizes requests on your behalf — whatever you write is
exactly the request(s) that go over the wire. `batchGetItem`/`batchWriteItem` are how you
explicitly ask for a request to be batched; `zipPar` is how you explicitly ask for two
independent queries to run in parallel. Anything you don't ask for stays sequential, one
request at a time. (2.x's `Zip` combinator used to batch or parallelize independent requests
silently under the hood — 3.x dropped that: see
["Why the rewrite"](../../index.md#why-the-rewrite) for the full reasoning.)

## Matrix

| Operation | Low-Level | High-Level | AWS SDK op |
|---|---|---|---|
| Get an item | [`getItem`](low-level.md#get) | [`get`](high-level.md#get) | [`GetItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html) |
| Put an item | [`putItem`](low-level.md#put) | [`put`](high-level.md#put) | [`PutItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html) |
| Update an item | [`updateItem`](low-level.md#update) | [`update`](high-level.md#update) | [`UpdateItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) |
| Delete an item | [`deleteItem`](low-level.md#delete) | [`deleteFrom`](high-level.md#delete) | [`DeleteItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html) |
| Query (by key condition) | [`query`](low-level.md#query) | [`query`](high-level.md#query) | [`Query`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) |
| Scan (whole table/index) | [`scan`](low-level.md#scan) | [`scan`](high-level.md#scan) | [`Scan`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html) |
| Batch get (up to 100 keys) | [`batchGetItem`](batch.md#batchgetitem) | [— Low-Level only](batch.md#why-no-high-level-batch-api) | [`BatchGetItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html) |
| Batch write (up to 25 puts/deletes) | [`batchWriteItem`](batch.md#batchwriteitem) | [— Low-Level only](batch.md#why-no-high-level-batch-api) | [`BatchWriteItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) |
| Transactional get (up to 100 items) | [`transactGetItems`](low-level.md#transactions) | [— not yet](high-level.md#transactions) | [`TransactGetItems`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html) |
| Transactional write (up to 100 items) | [`transactWriteItems`](low-level.md#transactions) | [— not yet](high-level.md#transactions) | [`TransactWriteItems`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html) |
| Create table | [`createTable`](low-level.md#table-management) | — Low-Level only | [`CreateTable`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html) |
| Delete table | [`deleteTable`](low-level.md#table-management) | — Low-Level only | [`DeleteTable`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html) |
| Describe table | [`describeTable`](low-level.md#table-management) | — Low-Level only | [`DescribeTable`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html) |

There's no High-Level wrapper for batch ops or transact ops, for two different reasons:

- **Batch is deliberately Low-Level only.** See
  [Batch Operations](batch.md#why-no-high-level-batch-api) for why batch's partial-success
  shape is best left to the caller to decide on.
- **Transactions simply haven't been built yet** for the High-Level API — see
  [High-Level: Transactions](high-level.md#transactions) for the current Low-Level-only
  workaround.

## Observability

Every operation above reports back typed capacity/metadata via a `ResponseInterceptor` if
one is attached — the same interceptor, the same metadata shape, regardless of which
interpreter (ZIO, Cats Effect, `Future`) or which API level (Low-Level or High-Level) you used
to build the query. See [Interceptor / Observability](../interceptor.md).
