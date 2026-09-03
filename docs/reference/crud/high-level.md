---
id: high-level
title: "CRUD — High-Level API"
---

The High-Level API is schema-derived: your own `case class`/`enum` models (with
`derives Schema`), `CompanionOptics`-generated `Lens`es instead of string field names, and
condition/key expressions checked against your model at compile time — a typo in a field
name, or comparing a field against the wrong type, is a compiler error, not a runtime
surprise. It compiles down to the exact same `DynamoDBQuery` ADT as the
[Low-Level API](low-level.md); nothing about using the High-Level API changes what goes over
the wire. Only `case class`/`enum`-shaped models are supported — see
[Limitations](../limitations.md) if your model doesn't fit that shape.

The examples below use the `dsl` import (see comment below), matching the
[landing page's examples](../../index.md#see-it-in-action).

```scala mdoc:silent
import zio.dynamodb._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*             // adds `.execute` to any DynamoDBQuery
import zio.dynamodb.blocks.ddbexpr.dsl.*        // get/put/update/... — same ops as DdbExprApi, one import

enum Status derives Schema {
  case Pending, Shipped
}

case class Order(customerId: String, orderId: String, total: Double, status: Status) derives Schema

object Order extends CompanionOptics[Order] {
  val customerId: Lens[Order, String] = $(_.customerId)
  val orderId: Lens[Order, String]    = $(_.orderId)
  val total: Lens[Order, Double]      = $(_.total)
  val status: Lens[Order, Status]     = $(_.status)
}

val orders = Table[Order]("orders")
```

Each operation takes a `Table[A]` rather than a table-name `String`: build one
`Table[Order]("orders")` and pass it to `get`/`put`/`query`/… . `Schema[Order]` is the only
implicit needed (it comes from `derives Schema`); the row codec is derived once and held on
the value. Passing the `Table` is also what lets the element type be inferred for
`query`/`scan`, which name it nowhere else.

### Configuring the codec

Attach deriver configuration to the `Table` with `.deriving` — no implicit
`DynamoDBCodecDeriverConfigure` in scope, the config is on the value:

```scala mdoc:compile-only
import zio.dynamodb.blocks.ddbexpr.dsl.*
import zio.blocks.schema.NameMapper

val ordersSnake =
  Table[Order]("orders").deriving(
    _.withFieldNameMapper(NameMapper.SnakeCase).withEnumValuesAsStrings(false)
  )
```

Order deriver-wide flags (`withFieldNameMapper`, `withEnumValuesAsStrings`,
`withSchema1TupleCompatibility`, …) before any per-field `withModifier` / `withInstance`
in the lambda. `.deriving` replaces rather than composes — express the whole config in one
call.

The rest of this page assumes `Order`/`Status`/`orders` as defined above, matching the
`examples` module's [`OrdersCE`/`OrdersZio`](../examples.md).

## Get

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  get(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1").execute
```

`get` returns `Either[DynamoDBError.ItemError, Order]`, not `Option[Order]` — a missing item
decodes as `Left(ItemError.ValueNotFound(...))`, in the same value, not a separate `None`
case to match on.

## Put

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  put(orders, Order("cust-42", "ord-1", 129.99, Status.Pending)).execute
```

## Update

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  update(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
    Order.status.set(Status.Shipped)
  ).execute
```

The compiler checks the field and the value being set together — `Order.status.set(129.99)`
wouldn't compile.

## Delete

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  deleteFrom(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1").execute
```

## Query

`query`/`scan` return the same `Page[A]` type as their Low-Level counterparts, just
parameterized by your model instead of `Item` — see
[Low-Level: Query](low-level.md#query) for the field-by-field breakdown of `Page`.

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  query(orders, limit = 20)
    .whereKey(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey > "ord-0")
    .filter(Order.total > 50.0)
    .execute
```

Each item in the returned page decodes independently, so `query`/`scan` return a page of
`Either[DynamoDBError.ItemError, Order]` rather than failing the whole page if one item's
decode fails.

## Scan

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  scan(orders, limit = 20).filter(Order.status === Status.Pending).execute
```

## Condition & key expressions

`Order.customerId.partitionKey === "cust-42"`, `Order.orderId.sortKey > "ord-0"`,
`Order.total > 50.0` — every comparison above is a plain Scala expression over the `Lens`es
`CompanionOptics` generated, not a hand-written expression string. This is the
`schema-ddbexpr` module's `DdbExpr`/`DdbKeyExpr` machinery: `===`/`>`/`<`/`>=`/`<=` work
directly on any `Lens` from a `CompanionOptics` object, including against `enum`/sealed-trait
fields (`Order.status === Status.Pending` above) — the interpreter derives the field's
`DynamoDBCodec` from the embedded `Schema` at evaluation time, so encoding rules like
`enumValuesAsStrings` are respected automatically, the same as `put`/`get` use.

## Transactions

Transactions (`transactGetItems` / `transactWriteItems`) are Low-Level only, the same
deliberate choice as [batch](batch.md#why-no-high-level-batch-or-transaction-api):
`transactWriteItems` is all-or-nothing and `transactGetItems` returns a positional
`Chunk[Option[Item]]` spanning tables, so a schema-typed wrapper would have to pick a
result shape and failure policy for a call whose point is heterogeneity. Build the
sub-operations with the Low-Level constructors, even in code that otherwise uses the
High-Level API throughout:

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactWriteItems(
      DynamoDBQuery.putItem("orders", Item("customerId" -> "cust-42", "orderId" -> "ord-2", "total" -> 42.0)),
      DynamoDBQuery.updateItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"))(
        ProjectionExpression.$("status").set("Shipped")
      )
    )
    .execute
```

For the read side, decode each returned `Item` with the same `Table` you pass to `get` —
`orders.decode(item)` — so the typed result uses the same codec configuration as the rest
of your High-Level code:

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def readExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactGetItems(
      DynamoDBQuery.GetItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"))
    )
    .execute
    .map(_.collect { case Some(item) => orders.decode(item) })
```
