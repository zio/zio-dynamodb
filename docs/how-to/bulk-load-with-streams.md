---
id: bulk-load-with-streams
title: "How to Bulk-Load and Bulk-Read Items With a Stream"
sidebar_label: "Bulk-Load With a Stream"
description: "Write or read large numbers of DynamoDB items from a ZStream with automatic BatchWriteItem and BatchGetItem batching."
keywords:
  - "Bulk Load"
  - "Batching"
  - "ZStream"
  - "batchWriteFromStream"
  - "batchReadFromStream"
---

In this guide, we import a stream of customer records into DynamoDB and look them up again by id, using the streaming helpers in the `zio.dynamodb` package. By the end, you will have a bulk import that sends `BatchWriteItem` requests of up to 25 items, several at a time, and a lookup that sends `BatchGetItem` requests of up to 100 keys, with unprocessed items retried for you.

We start with a plain bulk write, tune how many batches run at once, read items back as case classes and as raw items, set a retry policy, and finally handle batch failures.

## The Problem

Writing items one `PutItem` at a time is slow and expensive for an import of thousands of records. DynamoDB's batch APIs are faster, but they come with rules: at most 25 writes per `BatchWriteItem`, at most 100 keys per `BatchGetItem`, and a response that may hand back "unprocessed" items, which the caller must resend with a back-off.

With the plain AWS SDK, a bulk import looks like this:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, BatchWriteItemRequest, PutRequest, WriteRequest }
import scala.jdk.CollectionConverters._

def importCustomers(client: DynamoDbClient, customers: List[(String, String, String)]): Unit =
  customers.grouped(25).foreach { group =>
    val writes = group.map { case (id, name, city) =>
      val item = Map(
        "id"   -> AttributeValue.builder().s(id).build(),
        "name" -> AttributeValue.builder().s(name).build(),
        "city" -> AttributeValue.builder().s(city).build()
      ).asJava
      WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build()
    }

    def send(pending: java.util.Map[String, java.util.List[WriteRequest]]): Unit =
      if (!pending.isEmpty) {
        val response = client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(pending).build())
        send(response.unprocessedItems()) // retries forever, with no back-off
      }

    send(Map("customers" -> writes.asJava).asJava)
  }
```

The grouping, the encoding, the retry loop and its missing back-off all live in application code, and the groups run one after another.

The ZIO DynamoDB helpers take a `ZStream` of input values and a function that turns one value into a query, and handle the rest.

## Prerequisites

The helpers are part of the core module:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The examples need a DynamoDB with a `customers` table whose partition key is `id`; [How to Connect to DynamoDB Local](connect-to-dynamodb-local.md) shows how to start one. They share these imports and model:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.put
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.ZStream

case class Customer(id: String, name: String, city: String)
object Customer {
  implicit val schema: Schema.CaseClass3[String, String, String, Customer] = DeriveSchema.gen[Customer]

  val (id, name, city) = ProjectionExpression.accessors[Customer]
}

val customers: ZStream[Any, Nothing, Customer] =
  ZStream.range(1, 1001).map(i => Customer(s"c-$i", s"Customer $i", "London"))
```

## Writing a Stream of Items

`batchWriteFromStream(stream)(f)` groups the stream into chunks of 25, turns each element into a write with `f`, and sends each chunk as one `BatchWriteItem`. The function must return a write: `DynamoDBQuery.put` or `DynamoDBQuery.deleteFrom`, or their low-level forms `DynamoDBQuery.putItem` and `DynamoDBQuery.deleteItem`.

To import every customer:

```scala mdoc:silent
val importAll: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(customers) { customer =>
    put("customers", customer)
  }.runDrain
```

The helper returns a stream of the queries' results, one per input element. A batched `DynamoDBQuery.put` has no return value, so the elements are `None`; `runDrain` simply runs the import. The stream's error type is `Throwable`; failures that come from DynamoDB are `DynamoDBError` values.

## Tuning Parallelism With `mPar`

The second parameter, `mPar`, is the number of batches in flight at the same time. It defaults to 10, so up to 250 items are being written at any moment.

To lower the load on a table with little provisioned write capacity:

```scala mdoc:silent
val gentleImport: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(customers, mPar = 2) { customer =>
    put("customers", customer)
  }.runDrain
```

## Reading Items for a Stream of Keys

`batchReadFromStream(tableName, stream)(pk)` is the read counterpart: it groups the input into chunks of 100, builds a `get` for each element from the primary key expression `pk` returns, and sends one `BatchGetItem` per chunk.

Each output element pairs the input with the item found, which makes the helper behave like a left outer join: a missing item is `None`, not a failure. Decoding errors stay in the stream as `Left` values so that one bad item does not stop the whole read.

To look up a stream of customer ids and keep only the ones that exist:

```scala mdoc:silent
val ids: ZStream[Any, Nothing, String] = ZStream("c-1", "c-2", "missing-id")

val found: ZStream[DynamoDBExecutor, Throwable, Customer] =
  batchReadFromStream("customers", ids)(id => Customer.id.partitionKey === id)
    .mapZIO(ZIO.fromEither(_))
    .collect { case (_, Some(customer)) => customer }
```

`ZIO.fromEither` turns a decoding error into a stream failure; use `collect { case Right(...) => ... }` instead if you want to skip undecodable items.

## Reading Raw Items

`batchReadItemFromStream` works like `batchReadFromStream` but uses the low-level API: the key function returns a `PrimaryKey` and each result is an `Option[Item]`. It suits tables without a Scala model, or migrations that copy items as they are.

To read raw items for the same ids:

```scala mdoc:silent
val rawItems: ZStream[DynamoDBExecutor, Throwable, (String, Option[Item])] =
  batchReadItemFromStream("customers", ids)(id => PrimaryKey("id" -> id))
```

## Retrying Unprocessed Items

When DynamoDB returns unprocessed items or keys, the library resends only those, following a retry policy. The default is up to three retries with a jittered exponential back-off starting at 50 milliseconds. A batch inherits the policy of the first query in it that sets one, so we set it on the query that `f` returns with `DynamoDBQuery#withRetryPolicy`.

To retry each batch up to 10 times with a longer back-off:

```scala mdoc:silent
val patientImport: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(customers) { customer =>
    put("customers", customer)
      .withRetryPolicy(Schedule.recurs(10) && Schedule.exponential(200.millis))
  }.runDrain
```

## Handling Batch Failures

If items are still unprocessed when the retries run out, the stream fails with `DynamoDBError.BatchError.WriteError` for writes or `DynamoDBError.BatchError.GetError` for reads. `WriteError#unprocessedItems` maps each table name to the writes that did not reach DynamoDB; `GetError#unprocessedKeys` maps each table name to the keys that were not read. The stream stops at the first failed batch.

To log the items that were not written before failing the import:

```scala mdoc:silent
val reportedImport: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(customers) { customer =>
    put("customers", customer)
  }.tapError {
    case DynamoDBError.BatchError.WriteError(unprocessed) =>
      ZIO.logError(s"not written: ${unprocessed.values.map(_.size).sum} items")
    case _                                                =>
      ZIO.unit
  }.runDrain
```

:::warning[Conditional writes are never batched]
`BatchWriteItem` does not accept condition expressions. If `f` returns a write with `DynamoDBQuery#where`, or one with `DynamoDBQuery#returns` set to anything other than `ReturnValues.None`, a chunk that holds two or more such writes fails with `DynamoDBError.BatchError.UnbatchableQueryError`, whose message lists the reasons. For conditional writes, run each one on its own, for example with `ZStream#mapZIOPar(n)(c => put("customers", c).where(Customer.id.notExists).execute)`.
:::

## Going Further

- [Auto Batching and Parallelisation](../reference/auto-batching-and-parallelisation.md) explains which queries can share a batch and the AWS limits behind the 25 and 100 sizes.
- [Error Handling](../reference/error-handling.md) describes every `DynamoDBError` case.
- The Cats Effect module offers the same helpers for FS2 streams as `batchWriteFromStreamF`, `batchReadFromStreamF` and `batchReadItemFromStreamF`; see [How to Use ZIO DynamoDB From Cats Effect](use-from-cats-effect.md).
