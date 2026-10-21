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
`batchWriteItem` go through two independent retry loops, each with its own attachment point:
effect-level (`.withRetryPolicy`, throttling/network errors on a given attempt) and
batch unprocessed (`.withBatchUnprocessedRetryPolicy`, re-submitting unprocessed keys/items from a
successful response). Each knob governs only its own loop. Unprocessed keys/items are
resubmitted by default — the interpreter's `defaultBatchUnprocessedRetryPolicy` is
`RetryPolicy.fullJitter` with a 100 ms base unless set otherwise in `fromAsyncClient` (`None` turns it
off); `.withBatchUnprocessedRetryPolicy(...)` overrides it per call. See
[Retries: batch operations](../retries.md#batch-operations-two-independent-loops) for why that
split exists — in short, the batch unprocessed retry, unlike the effect-level one, is safe to use
even with the SDK client's own retries left on.

## Item settings

AWS takes capacity and item collection metrics only for the whole batch: an entry in a batch write
is just an item (put) or a key (delete), and a batch get lists keys per table. So set `.capacity`,
`.metrics` and `.withRetryPolicy` on the batch; the same settings on an individual item fail the
batch with `DynamoDBError.QueryBuilderError.UnsupportedModifier` when it runs, before any request is
sent.

A get's `.consistency(ConsistencyMode.Strong)` is sent as AWS's per-table `ConsistentRead`, so every
get on a table must ask for the same consistency; gets on one table that differ fail the batch with
`UnbatchableQuery`. A `batchGetItem` callback that doesn't return a `getItem` fails it with
`UnbatchableQuery` too.

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
`Write` per element. `withRetryPolicy` is optional — it retries failed requests and, when
omitted, falls back to the interpreter's `defaultRetryPolicy`, which is `None` (no retry)
unless the interpreter was constructed with one. It doesn't affect resubmission of unprocessed
keys/items. See [Retries](../retries.md).

## batchGetItem

Running a `BatchGetItem` query produces a `Batch.GetResult`, not a plain `Chunk`/`List` of
items — `Incomplete`/`Failed` are successful effect outcomes carrying AWS's partial-failure
detail as data, not raised errors, so you pattern-match on it rather than parse an exception:

```scala
sealed trait GetResult
object GetResult {
  final case class Complete(response: BatchGetItem.Response) extends GetResult
  final case class Incomplete(response: BatchGetItem.Response) extends GetResult
  final case class Failed(cause: Throwable, unprocessedRetries: Int, effectRetries: Int) extends GetResult
}
```

- **`Complete`** — every requested key was retrieved (or confirmed absent); nothing left over.
- **`Incomplete`** — the retry policy ran out with keys still unprocessed. This is not a
  failure: every AWS call that did happen succeeded, DynamoDB simply kept returning leftovers
  (throttling, internal capacity) faster than the policy retried. `response.unprocessedKeys`
  tells you what's still outstanding.
- **`Failed`** — an effect-level failure (throttling exception, network error) survived every
  effect-level retry. `unprocessedRetries`/`effectRetries` tell you how far the batch got before
  giving up.

## batchWriteItem

`batchWriteItem` mirrors `batchGetItem` exactly, with `Batch.WriteResult` in place of
`Batch.GetResult`:

```scala
sealed trait WriteResult
object WriteResult {
  final case class Complete(response: BatchWriteItem.Response) extends WriteResult
  final case class Incomplete(response: BatchWriteItem.Response) extends WriteResult
  final case class Failed(cause: Throwable, unprocessedRetries: Int, effectRetries: Int) extends WriteResult
}
```

Same three cases, same meaning — `Incomplete` carries `response.unprocessedItems` (a table →
pending-put/delete map) in place of `unprocessedKeys`.

## Batch and the High-Level API

There's no schema-derived `batch`/`batchGet` in `DdbExprApi`/`dsl` — batch is Low-Level only.
`batchGetItem`/`batchWriteItem`, `RetryPolicy`, and `Batch.GetResult`/`Batch.WriteResult` are
the building blocks for whatever you need on top. See
[`ZIOStreamingUtils.batchGetItems`](../examples.md) in the `examples` module for a worked
example: batching a stream of keys, running each batch with a retry policy, and turning
`Incomplete`/`Failed` into log output rather than a fatal error.

To type the raw `Item`s that come back from a `Batch.GetResult`, call `Table#decode` /
`Table#encode` on the same `Table` value you use for High-Level `get`/`put`. That reuses the
table's configured, cached codec, so the hand-rolled path stays consistent with the rest of
your High-Level code instead of re-deriving a codec by hand.
