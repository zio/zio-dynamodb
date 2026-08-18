---
id: low-level
title: "CRUD — Low-Level API"
---

The Low-Level API is shaped directly after the AWS SDK: table names are plain strings,
items are `Item`/`PrimaryKey` maps (`AttrMap` under the hood), and fields are referenced by
name via `$("fieldName")` rather than a schema-derived `Lens`. No `Schema` instance is
required — this is the API to reach for when your data doesn't have (or doesn't need) a
compile-time model, or when you want the AWS request shape to stay directly visible in your
code.

All examples below build a query value; nothing executes until `.execute` is called with an
`Interpreter[F]` in scope (see the [ZIO](../../index.md#see-it-in-action)/
[Cats Effect](../../index.md#see-it-in-action) examples on the landing page for how to obtain
one).

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$
```

## Get

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery.getItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1")).execute
```

`getItem` also accepts `ProjectionExpression`s to fetch only specific fields:

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .getItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"), $("total"), $("status"))
    .execute
```

## Put

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .putItem("orders", Item("customerId" -> "cust-42", "orderId" -> "ord-1", "total" -> 129.99, "status" -> "Pending"))
    .execute
```

A condition expression makes `putItem` conditional (fails the request, doesn't throw
client-side, if the condition isn't met on the server) — replacing an item only if it's still
in the state you expect, for example:

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .putItem(
      "orders",
      Item("customerId" -> "cust-42", "orderId" -> "ord-1", "total" -> 129.99, "status" -> "Shipped"),
      conditionExpression = Some($("status") === "Pending")
    )
    .execute
```

## Update

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .updateItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"))($("status").set("Shipped"))
    .execute
```

Update actions compose with `+`/`%` (see `UpdateExpression.Action`) — `.set`, `.add`,
`.appendList`, `.deleteFromSet`, and `.remove` are all available on any `ProjectionExpression`,
Low-Level or High-Level, since both build the same `Action` values under the hood.

## Delete

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery.deleteItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1")).execute
```

## Query

`querySome` requires a partition key condition (via `.whereKey`) and returns one `Page[Item]`
at a time — `lastEvaluatedKey` on the result tells you whether to page again.

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .querySome("orders", limit = 20)
    .whereKey($("customerId").partitionKey === "cust-42" && $("orderId").sortKey > "ord-0")
    .filter($("total") > 50.0)
    .execute
```

## Scan

`scanSome` reads the whole table (or index) a page at a time — no key condition required,
but correspondingly no way to target a specific partition efficiently. Prefer `query` when
you know the partition key.

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .scanSome("orders", limit = 20)
    .filter($("status") === "Pending")
    .execute
```

## Transactions

`transactGetItems`/`transactWriteItems` compose multiple single-item operations into one
atomic, all-or-nothing call — up to 100 items across tables. Unlike batch (see
[Batch Operations](batch.md)), there's no partial-success outcome to represent: either every
sub-operation succeeds, or the whole transaction is cancelled and the effect fails.

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*

def readExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactGetItems(
      DynamoDBQuery.GetItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1")),
      DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> "cust-42"))
    )
    .execute

def writeExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactWriteItems(
      DynamoDBQuery.putItem("orders", Item("customerId" -> "cust-42", "orderId" -> "ord-2", "total" -> 42.0)),
      DynamoDBQuery.updateItem("customers", PrimaryKey("customerId" -> "cust-42"))(
        zio.dynamodb.ProjectionExpression.$("orderCount").add(1)
      )
    )
    .execute
```

A failed transaction surfaces as `DynamoDBError.TransactionError.TransactionCancelled(reasons)`
in the effect's error channel — a normal typed failure, carrying one `CancellationReason` per
sub-operation (code, optional message, and the conflicting item if
`ReturnValuesOnConditionCheckFailure.AllOld` was requested on that action).

## Table management

`createTable`/`deleteTable`/`describeTable` round out the Low-Level API for table lifecycle —
uncommon in application code (most tables are managed via infrastructure-as-code), but useful
for tests and local development. `deleteTable("orders")`/`describeTable("orders")` take just
a table name; `createTable` additionally takes a `KeySchema`, a set of `AttributeDefinition`s,
and a `BillingMode`.
