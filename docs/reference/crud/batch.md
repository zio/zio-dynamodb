---
id: batch
title: "Batch Operations"
---

Batch ops get their own page rather than a couple of rows in the [matrix](index.md) because
they don't fit the Low-Level/High-Level split cleanly, and because their error-handling shape
is genuinely different from the rest of the library — both are worth explaining once, up
front, instead of as a surprise mid-example.

## Why batch is more complex

The AWS batch APIs (`BatchGetItem`, `BatchWriteItem`) already have a larger surface area than
their single-item counterparts — up to 100 keys or 25 writes per call, per-table grouping, and
a response shape that separates out **unprocessed** items DynamoDB didn't get to (throttling,
internal capacity limits) from items it actually handled. On top of that, this library adds:

- **Built-in retry.** `DynamoDBQuery.batchGetItem`/`batchWriteItem` accept a `RetryPolicy` and
  the interpreter honors the AWS batch retry contract internally — resubmitting unprocessed
  keys/items on your behalf until either everything is processed or the policy is exhausted.
  Single-item ops don't do this; a batch, by AWS's own design, routinely needs more than one
  round trip to finish.
- **Two independent retry loops.** Effect-level (transient failures — throttling, network — on
  each individual attempt) and response-level (re-submitting unprocessed items) are both driven
  inside the interpreter, both governed by the same attached `RetryPolicy`.

None of this is optional complexity added for its own sake — it's what "make a batch call that
actually finishes" requires once you take AWS's partial-failure contract seriously.

## Building a batch

```scala mdoc:compile-only
import zio.dynamodb._
import zio.dynamodb.ExecuteSyntax.*
import scala.concurrent.duration.DurationInt

val people = List("alice", "bob", "carol")

def getExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchGetItem(people)(id => DynamoDBQuery.GetItem("customers", PrimaryKey("customerId" -> id)))
    .withRetryPolicy(RetryPolicy.ExponentialBackoff(maxRetries = 5, initialDelay = 50.millis))
    .execute

def writeExample(implicit interp: Interpreter[zio.Task]) =
  DynamoDBQuery
    .batchWriteItem(people)(id => DynamoDBQuery.putItem("customers", Item("customerId" -> id, "active" -> true)))
    .withRetryPolicy(RetryPolicy.NoRetry)
    .execute
```

`batchGetItem`/`batchWriteItem` fold an `Iterable[A]` into one batch query, one `GetItem`/
`Write` per element. `withRetryPolicy` is optional — omitting it is equivalent to
`RetryPolicy.NoRetry`: the batch runs once, with whatever AWS returns as unprocessed left
unprocessed.

## batchGetItem

Running a `BatchGetItem` query produces a `Batch.GetResult`, not a plain `Chunk`/`List` of
items:

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

## Why errors-as-values, not a failed effect

Every other operation in this library reports failure through the normal effect error channel
— a failed `Task`/`IO`. Batch is the one deliberate exception: `Incomplete` and `Failed` are
both *successful* effect outcomes carrying a result value, not a raised error.

This is a conscious trade-off, not an oversight. AWS's batch APIs return unprocessed
keys/items as first-class response data, with enough detail (which keys, which table) to act
on programmatically — retry them differently, log them, drop them, escalate. Collapsing that
into a single "the batch failed" exception would throw away information AWS is handing you for
free. `Batch.GetResult`/`Batch.WriteResult` keep that information intact and make the
incomplete/partial-success case something you pattern-match on rather than something you have
to `catch` and re-parse out of an exception.

The cost is that this genuinely differs from what the rest of the library trains you to
expect — reading `interp.run(query)` doesn't tell you whether a batch result needs a `match` on
top before you know if it actually succeeded. Worth calling out explicitly once, here, rather
than a surprise the first time it comes up.

## Why no High-Level batch or transaction API

There's no schema-derived `batch`/`batchGet` in `DdbExprApi`/`dsl` — batch stays Low-Level
only. Once a batch call can come back `Incomplete`, the caller has to decide what to do about
the leftover keys/items, and that decision is genuinely use-case specific: retry immediately,
retry with backoff, drop and log, surface to the user, feed into a dead-letter queue. Baking
one of those choices into a High-Level wrapper would mean picking a policy on the library's
behalf for a case where "it depends" is the honest answer.

The building blocks — `batchGetItem`/`batchWriteItem`, `RetryPolicy`, `Batch.GetResult`/
`Batch.WriteResult` — are all there to build whatever policy fits. See
[`ZIOStreamingUtils.batchGetItems`](../examples.md) in the `examples` module for a worked
example: batching a stream of keys, running each batch with a retry policy, and turning
`Incomplete`/`Failed` into log output rather than a fatal error.

The same reasoning covers **transactions**. `transactWriteItems` is all-or-nothing (no
partial-failure shape to model) and `transactGetItems` returns a positional
`Chunk[Option[Item]]` spanning tables — a schema-typed wrapper would have to pick a result
shape (grouped? tuple? per-position?) for a call whose point is heterogeneity. Build both
from the Low-Level constructors.

To type the raw `Item`s that come back — from a `Batch.GetResult` or a `transactGetItems`
chunk — call `Table#decode` / `Table#encode` on the same `Table` value you use for
High-Level `get`/`put`. That reuses the table's configured, cached codec, so the
hand-rolled path stays consistent with the rest of your High-Level code instead of
re-deriving a codec by hand.
