---
id: transactions
title: "Transactions"
---

`transactGetItems`/`transactWriteItems` compose multiple single-item operations into one
atomic, all-or-nothing call across up to 100 items, across tables. Unlike [batch](batch.md),
there's no partial-success outcome to represent: either every sub-operation succeeds, or the
whole transaction is cancelled and the effect fails. Both are Low-Level only — see
[Batch Operations](batch.md#batch-and-the-high-level-api) for why, and how to bridge to your
models via `Table#decode`/`Table#encode`.

## transactGetItems

Reads multiple items, across tables, in one atomically-consistent call:

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
```

Returns a positional `Chunk[Option[Item]]` — one slot per requested key, `None` where the
item doesn't exist, in the same order the keys were given. To type the raw `Item`s that come
back, call `Table#decode` on the same `Table` you use for High-Level `get`/`put` — that reuses
the table's configured, cached codec, so the hand-rolled path stays consistent with the rest
of your High-Level code.

## transactWriteItems

Writes/deletes/condition-checks up to 100 items atomically, from Low-Level constructors:

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.ProjectionExpression.$

def example(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .transactWriteItems(
      DynamoDBQuery.putItem("orders", Item("customerId" -> "cust-42", "orderId" -> "ord-2", "total" -> 42.0)),
      DynamoDBQuery.deleteItem("customers", PrimaryKey("customerId" -> "cust-41")),
      DynamoDBQuery.conditionCheck("orders", PrimaryKey("customerId" -> "cust-42", "orderId" -> "ord-1"))(
        $("status") === "open"
      )
    )
    .execute
```

`conditionCheck` is a condition-only action with no mutation of its own, used to guard the
whole transaction on another item's state. `.withClientRequestToken(token)` makes a retry of
the exact same transaction idempotent — DynamoDB returns the original result instead of
re-applying it. `.returnValuesOnConditionCheckFailure(AllOld)` on a write or `conditionCheck`
action returns that item's prior state if it's the one that caused the cancellation.

## Errors

A failed transaction surfaces as `DynamoDBError.TransactionError.TransactionCancelled(reasons)`
on the normal failed-effect channel — not errors-as-values like batch, since there's no
partial-success state to carry. One `CancellationReason` per sub-operation (code, optional
message, and the conflicting item if `ReturnValuesOnConditionCheckFailure.AllOld` was
requested on that action).
