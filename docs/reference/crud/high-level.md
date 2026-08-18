---
id: high-level
title: "CRUD — High-Level API"
---

The high-level (HL) API is schema-derived: your own `case class`/`enum` models (with
`derives Schema`), `CompanionOptics`-generated `Lens`es instead of string field names, and
condition/key expressions checked against your model at compile time — a typo in a field
name, or comparing a field against the wrong type, is a compiler error, not a runtime
surprise. It compiles down to the exact same `DynamoDBQuery` ADT as the
[Low-Level API](low-level.md); nothing about using the HL API changes what goes over the
wire.

`DdbExprApi`/`dsl` are two names for the same operations — `dsl` is a facade meant for a
single `import zio.dynamodb.blocks.ddbexpr.dsl.*`, `DdbExprApi` is the underlying object if
you'd rather import it explicitly. The examples below use the `dsl` import, matching the
[landing page's examples](../../index.md#see-it-in-action).

```scala mdoc:silent
import zio.dynamodb._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

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
```

The rest of this page assumes `Order`/`Status` as defined above, matching the
`examples` module's [`OrdersCE`/`OrdersZio`](../examples.md).

## Get

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  get[Order]("orders")(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1").execute
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
  put("orders", Order("cust-42", "ord-1", 129.99, Status.Pending)).execute
```

## Update

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  update("orders")(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
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
  deleteFrom[Order]("orders")(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1").execute
```

## Query

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

def example(implicit interp: Interpreter[zio.Task]) =
  query[Order]("orders", limit = 20)
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
  scan[Order]("orders", limit = 20).filter(Order.status === Status.Pending).execute
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

There's no HL way to build a transaction. `transactGetItems`/`transactWriteItems` require
`DynamoDBQuery[Any, _]`-shaped sub-operations (see
[Low-Level: Transactions](low-level.md#transactions)), but `get`/`put`/`update`/`deleteFrom`
above produce `DynamoDBQuery[Order, _]` — pinned to your model type, not `Any`. Since
`DynamoDBQuery`'s input parameter is contravariant, an `Order`-shaped query isn't a subtype of
an `Any`-shaped one, so it can't be passed to `transactWriteItems` directly.

Build transaction sub-operations with the LL constructors instead, even in code that otherwise
uses the HL API throughout:

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
