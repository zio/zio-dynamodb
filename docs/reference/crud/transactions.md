---
id: transactions
title: "Transactions"
---

`transactGetItems`/`transactWriteItems` compose multiple single-item operations into one
atomic, all-or-nothing call across up to 100 items, across tables. Unlike [batch](batch.md),
there's no partial-success outcome to represent: either every sub-operation succeeds, or the
whole transaction is cancelled and the effect fails. `transactGetItems` is Low-Level only;
`transactWriteItems` accepts High-Level values (`DdbExprApi.put`/`update`/`deleteFrom`/
`conditionCheck`) mixed freely with Low-Level constructors.

```scala mdoc:silent
import zio.dynamodb._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

case class Order(customerId: String, orderId: String, total: Double, status: String) derives Schema

object Order extends CompanionOptics[Order] {
  val customerId: Lens[Order, String] = $(_.customerId)
  val orderId: Lens[Order, String]    = $(_.orderId)
  val status: Lens[Order, String]     = $(_.status)
}

val orders = Table[Order]("orders")
```

## transactGetItems

Reads multiple items, across tables, in one atomically-consistent call — Low-Level only:

```scala mdoc:compile-only
def readExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactGetItems(
      DynamoDBQuery.GetItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1")),
      DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> "cust-42"))
    )
    .execute
```

Returns a positional `Chunk[Option[Item]]` — one slot per requested key, `None` where the
item doesn't exist, in the same order the keys were given. To type the raw `Item`s that come
back, call `Table#decode` on the same `Table` you use for High-Level `get`/`put` — that reuses
the table's configured, cached codec, so the hand-rolled path stays consistent with the rest
of your High-Level code:

```scala mdoc:compile-only
def decodeExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactGetItems(
      DynamoDBQuery.GetItem("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"))
    )
    .execute
    .map(_.collect { case Some(item) => orders.decode(item) })
```

## transactWriteItems

Writes/updates/deletes up to 100 items atomically, and accepts High-Level values directly —
`DdbExprApi.put`/`update`/`deleteFrom` (or the equivalent `dsl` imports) compose into the same
call as Low-Level `putItem`/`updateItem`/`deleteItem`, even mixing models from different
tables in one transaction:

```scala mdoc:compile-only
def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactWriteItems(
      put(orders, Order("cust-42", "ord-2", 42.0, "open")),
      update(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
        Order.status.set("shipped")
      ),
      DynamoDBQuery.deleteItem("customers", PrimaryKey("customerId" -> "cust-41")) // Low-Level, mixed in freely
    )
    .execute
```

`conditionCheck` is a condition-only action with no mutation of its own, used to guard the
whole transaction on another item's state — also available as a High-Level builder,
interpreted with the calling `Table`'s deriver configuration exactly like `.where`:

```scala mdoc:compile-only
def guardedExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactWriteItems(
      conditionCheck(orders)(Order.customerId.partitionKey === "cust-42" && Order.orderId.sortKey === "ord-1")(
        Order.status === "open"
      ),
      put(orders, Order("cust-42", "ord-2", 42.0, "open"))
    )
    .execute
```

`.withClientRequestToken(token)` makes a retry of the exact same transaction idempotent —
DynamoDB returns the original result instead of re-applying it.
`.returnValuesOnConditionCheckFailure(AllOld)` on a write or `conditionCheck` action returns
that item's prior state if it's the one that caused the cancellation.

## Errors

A failed transaction surfaces as `DynamoDBError.TransactionError.TransactionCancelled(reasons)`
on the normal failed-effect channel — not errors-as-values like batch, since there's no
partial-success state to carry. One `CancellationReason` per sub-operation (code, optional
message, and the conflicting item if `ReturnValuesOnConditionCheckFailure.AllOld` was
requested on that action). A High-Level value whose optic path can't resolve to a DynamoDB
attribute (e.g. a non-`String` map key) fails the same way `put`/`update`/`get` do standalone
— with a `DecodingError`, before any AWS request is issued, never as a silently incomplete
write.
