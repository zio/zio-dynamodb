---
id: interceptor
title: "Interceptor / Observability"
---

`ResponseInterceptor[F]` is a callback invoked after every DynamoDB data operation completes —
the mechanism behind the "Observability" note on the [CRUD matrix](crud/index.md#observability):
the same interceptor, the same metadata shape, regardless of interpreter (ZIO, Cats Effect,
`Future`) or which API level (Low-Level or High-Level) built the query.

```scala
trait ResponseInterceptor[F[_]] {
  def onResponse(meta: DynamoDBResponseMetadata): F[Unit]
}
```

`onResponse`'s effect is sequenced *before* the caller receives its result — side effects
(logging, metrics, rate limiting) are guaranteed to run, and can genuinely delay when a
sequential pipeline proceeds. DDL operations (`createTable`/`deleteTable`/`describeTable`)
produce no metadata and never invoke the interceptor.

## Attaching an interceptor

Pass it when constructing the interpreter — each interpreter has a `fromAsyncClient` overload
that takes one:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.dynamodb._

val logging: ResponseInterceptor[Task] = new ResponseInterceptor[Task] {
  def onResponse(meta: DynamoDBResponseMetadata): Task[Unit] =
    ZIO.logInfo(s"DynamoDB response: $meta")
}

val interp: ZioInterpreter =
  ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), logging)
```

`CEInterpreter.fromAsyncClient`/`FutureInterpreter.fromAsyncClient` take the same second
argument, typed to their own effect (`ResponseInterceptor[cats.effect.IO]`/
`ResponseInterceptor[scala.concurrent.Future]`). Omitting the interceptor argument entirely
(the single-argument `fromAsyncClient` overload) runs with none attached — no overhead, no
metadata collection.

Under the hood, attaching an interceptor also switches on `ReturnConsumedCapacity.TOTAL` (and
`ReturnItemCollectionMetrics.SIZE` for writes) on every request automatically — you don't
need to set those yourself for the metadata to be populated.

## What's in `DynamoDBResponseMetadata`

`DynamoDBResponseMetadata` is a sealed trait with one case class per operation
(`GetItem`/`PutItem`/`UpdateItem`/`DeleteItem`/`Query`/`Scan`/`BatchGetItem`/
`BatchWriteItem`/`TransactGetItems`/`TransactWriteItems`), each carrying:

- **`consumed: Option[ConsumedCapacity]`** (or `Chunk[ConsumedCapacity]` for batch/transact) —
  read/write capacity units, broken out by table and by any local/global secondary indexes
  involved. `None`/empty only if AWS didn't report capacity for that call.
- **`correlation: CorrelationContext`** (single-item ops only) — the primary key from the
  originating request, so a response can be tied back to what was asked for.
  `CorrelationContext.primaryKey` is `None` for `PutItem` specifically, since the key fields
  are embedded in the full item map and can't be separated without table schema knowledge.
- **`collectionMetrics: Option[ItemCollectionMetrics]`** (write ops only) — item collection
  size estimates, populated only for tables with a local secondary index.

See the scaladoc on `DynamoDBResponseMetadata`/`ConsumedCapacity`/`Capacity` for the full
field-by-field breakdown.

## Accumulating for tests

`ZioResponseInterceptor.accumulating`/`CEResponseInterceptor.accumulating`/
`FutureResponseInterceptor.accumulating` build a ready-made `Ref`-backed interceptor that
collects metadata in call order, for asserting on what a test run actually reported:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.dynamodb._

def example =
  for {
    acc    <- ZioResponseInterceptor.accumulating
    interp  = ZioInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build(), acc.interceptor)
    _      <- interp.run(DynamoDBQuery.getItem("orders", PrimaryKey("orderId" -> "ord-1")))
    seen   <- acc.results
  } yield seen
```

Create a fresh interceptor per request/test to isolate metadata collection — the accumulator
is shared by any fiber holding a reference to it.

## Worked example: rate limiting on consumed capacity

`examples/src/main/scala/zio/dynamodb/RateLimitedReads.scala` builds a token-bucket rate
limiter weighted by *RCUs actually consumed* per response (not raw call count — a 1-RCU call
and a 40-RCU call shouldn't be throttled identically), and pairs it with
[`ZIOStreamingUtils.batchGetItems`](crud/batch.md#why-no-high-level-batch-or-transaction-api): each
batch's `onResponse` delay gates when the stream pulls the next batch, so a sequential
`BatchGetItem` pipeline self-throttles against a target RCU budget with no external rate
limiter needed.
