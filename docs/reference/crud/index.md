---
id: index
title: "CRUD Operations"
---

## Orientation

Every operation below compiles down to the same `DynamoDBQuery` ADT and runs through the same
interpreters — the low-level (LL) and high-level (HL) APIs are two different ways to *build*
a query, not two different execution paths. You can mix both in the same program.

- **Low-level (LL)** — `DynamoDBQuery.getItem`/`putItem`/`updateItem`/... — shaped directly
  after the AWS SDK: string table names, `Item`/`PrimaryKey` maps, string-keyed projection
  expressions (`$("fieldName")`). No `Schema` required. See [Low-Level API](low-level.md).
- **High-level (HL)** — `DdbExprApi.get`/`put`/`update`/... — schema-derived: your own case
  classes, `CompanionOptics`-generated `Lens`es instead of string field names, compile-time
  checked condition/key expressions. Requires `derives Schema` on your model. See
  [High-Level API](high-level.md).

**Batching and parallelism are always explicit, never automatic.** `DynamoDBQuery` never
batches or parallelizes requests on your behalf — whatever you write is exactly the request(s)
that go over the wire. `batchGetItem`/`batchWriteItem` are how you explicitly ask for a
request to be batched; `zipPar` is how you explicitly ask for two independent queries to run
in parallel. Anything you don't ask for stays sequential, one request at a time. (2.x's `Zip`
combinator used to batch or parallelize independent requests silently under the hood — 3.x
dropped that: see ["Why the rewrite"](../../index.md#why-the-rewrite) for the full reasoning.)
This is *why* the matrix below has separate rows for single-item ops, batch ops, and
transactions instead of one op quietly covering all three — each row is a deliberate, visible
choice, not an implementation detail the library picked for you.

## Matrix

| Operation | LL | HL | AWS SDK op |
|---|---|---|---|
| Get an item | [`getItem`](low-level.md#get) | [`get`](high-level.md#get) | `GetItem` |
| Put an item | [`putItem`](low-level.md#put) | [`put`](high-level.md#put) | `PutItem` |
| Update an item | [`updateItem`](low-level.md#update) | [`update`](high-level.md#update) | `UpdateItem` |
| Delete an item | [`deleteItem`](low-level.md#delete) | [`deleteFrom`](high-level.md#delete) | `DeleteItem` |
| Query (by key condition) | [`querySome`](low-level.md#query) | [`query`](high-level.md#query) | `Query` |
| Scan (whole table/index) | [`scanSome`](low-level.md#scan) | [`scan`](high-level.md#scan) | `Scan` |
| Batch get (up to 100 keys) | [`batchGetItem`](batch.md#batchgetitem) | — LL only | `BatchGetItem` |
| Batch write (up to 25 puts/deletes) | [`batchWriteItem`](batch.md#batchwriteitem) | — LL only | `BatchWriteItem` |
| Transactional get (up to 100 items) | [`transactGetItems`](low-level.md#transactions) | [— LL only](high-level.md#transactions) | `TransactGetItems` |
| Transactional write (up to 100 items) | [`transactWriteItems`](low-level.md#transactions) | [— LL only](high-level.md#transactions) | `TransactWriteItems` |
| Create table | [`createTable`](low-level.md#table-management) | — LL only | `CreateTable` |
| Delete table | [`deleteTable`](low-level.md#table-management) | — LL only | `DeleteTable` |
| Describe table | [`describeTable`](low-level.md#table-management) | — LL only | `DescribeTable` |

There's no HL wrapper for batch ops, transact ops, or table management — see
[Batch Operations](batch.md) for why batch is deliberately LL-only,
[High-Level: Transactions](high-level.md#transactions) for why transact sub-operations must
be LL-shaped even in otherwise-HL code (a contravariance constraint, not a missing feature),
and [Limitations](../limitations.md) for the general FP-modeling-only shape of the HL API.

## Observability

Every operation above reports back typed capacity/metadata via a `ResponseInterceptor` if
one is attached — the same interceptor, the same metadata shape, regardless of which
interpreter (ZIO, Cats Effect, `Future`) or which API level (LL or HL) you used to build the
query. See [Interceptor / Observability](../interceptor.md).
