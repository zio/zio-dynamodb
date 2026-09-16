---
id: batch
title: "Batch Operations"
---

AWS reference: [`BatchGetItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html),
[`BatchWriteItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html).

## Retry behavior

AWS's batch APIs cap out at 100 keys (`BatchGetItem`) / 25 writes (`BatchWriteItem`) per call
and can return **unprocessed** items — ones DynamoDB didn't get to (throttling, internal
capacity limits) — separately from ones it handled. `DynamoDBQuery.batchGetItem`/
`batchWriteItem` accept a `RetryPolicy`; the interpreter resubmits unprocessed keys/items on
your behalf until either everything is processed or the policy is exhausted, via two
independent retry loops governed by the same attached `RetryPolicy`: effect-level (throttling,
network errors on a given attempt) and response-level (re-submitting unprocessed items).

## Building a batch

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import scala.concurrent.duration.DurationInt

case class Person(id: String, name: String)

val people    = List(Person("alice", "Alice"), Person("bob", "Bob"), Person("carol", "Carol"))
val personIds = people.map(_.id)

def getExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchGetItem(personIds)(id => DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> id)))
    .withRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries = 5, initialDelay = 50.millis))
    .execute

def writeExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchWriteItem(people)(person =>
      DynamoDBQuery.putItem("customers", Item("customerId" -> person.id, "active" -> true))
    )
    .withRetryPolicy(RetryPolicy.NoRetry)
    .execute
```

`batchGetItem`/`batchWriteItem` fold an `Iterable[A]` into one batch query, one `GetItem`/
`Write` per element. `withRetryPolicy` is optional — omitting it is equivalent to
`RetryPolicy.NoRetry`: the batch runs once, with whatever AWS returns as unprocessed left
unprocessed.

## batchGetItem

Running a `BatchGetItem` query produces a `Batch.GetResult`, not a plain `Chunk`/`List` of
items — `Incomplete`/`Failed` are successful effect outcomes carrying AWS's partial-failure
detail as data, not raised errors, so you pattern-match on it rather than parse an exception:

```scala
sealed trait GetResult
object GetResult {
  final case class Complete(response: BatchGetItem.Response) extends GetResult
  final case class Incomplete(response: BatchGetItem.Response) extends GetResult
  final case class Failed(cause: Throwable, responseRetries: Int, effectRetries: Int) extends GetResult
}
```

- **`Complete`** — every requested key was retrieved (or confirmed absent); nothing left over.
- **`Incomplete`** — the retry policy ran out with keys still unprocessed. This is not a
  failure: every AWS call that did happen succeeded, DynamoDB simply kept returning leftovers
  (throttling, internal capacity) faster than the policy retried. `response.unprocessedKeys`
  tells you what's still outstanding.
- **`Failed`** — an effect-level failure (throttling exception, network error) survived every
  effect-level retry. `responseRetries`/`effectRetries` tell you how far the batch got before
  giving up.

## batchWriteItem

`batchWriteItem` mirrors `batchGetItem` exactly, with `Batch.WriteResult` in place of
`Batch.GetResult`:

```scala
sealed trait WriteResult
object WriteResult {
  final case class Complete(response: BatchWriteItem.Response) extends WriteResult
  final case class Incomplete(response: BatchWriteItem.Response) extends WriteResult
  final case class Failed(cause: Throwable, responseRetries: Int, effectRetries: Int) extends WriteResult
}
```

Same three cases, same meaning — `Incomplete` carries `response.unprocessedItems` (a table →
pending-put/delete map) in place of `unprocessedKeys`.

## Batch and the High-Level API

Neither batch nor [transactions](transactions.md) have a schema-derived wrapper — both are
Low-Level only, for two reasons. First, batch's partial-success outcome and a transaction's
all-or-nothing, cross-table result shape each need a use-case-specific policy (retry now,
retry with backoff, drop and log, escalate) that the library can't pick on your behalf.
Second, going through a `Schema` adds a distinct failure channel of its own — a field or key
path that can't be represented as a `ProjectionExpression` — that neither result shape has a
slot for. Call `Table#encode`/`Table#decode` yourself around the Low-Level constructors to
get that channel's errors up front, on your own terms, rather than have it interleave with
retry/cancellation semantics that already exist.

`batchGetItem`/`batchWriteItem`, `RetryPolicy`, and `Batch.GetResult`/`Batch.WriteResult` are
the building blocks for whatever you need on top. See
[`ZIOStreamingUtils.batchGetItems`](../examples.md) in the `examples` module for a worked
example: batching a stream of keys, running each batch with a retry policy, and turning
`Incomplete`/`Failed` into log output rather than a fatal error.

To type the raw `Item`s that come back from a `Batch.GetResult`, call `Table#decode` /
`Table#encode` on the same `Table` value you use for High-Level `get`/`put`. That reuses the
table's configured, cached codec, so the hand-rolled path stays consistent with the rest of
your High-Level code instead of re-deriving a codec by hand.
