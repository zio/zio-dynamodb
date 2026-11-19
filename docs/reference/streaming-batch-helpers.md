---
id: streaming-batch-helpers
title: "Streaming Batch Helpers"
description: "batchWriteFromStream, batchReadFromStream and batchReadItemFromStream: batched DynamoDB reads and writes driven by a ZStream."
keywords:
  - "DynamoDB"
  - "ZIO Streams"
  - "Batching"
  - "batchWriteFromStream"
  - "batchReadFromStream"
---

The streaming batch helpers **write or read an unbounded number of items in DynamoDB batches, driven by a `ZStream`**. `DynamoDBQuery.batch` batches a collection that is already in memory, and AWS limits one `BatchWriteItem` to 25 writes and one `BatchGetItem` to 100 keys. The helpers in the `zio.dynamodb` package object take care of both: they group the stream into chunks of the maximum batch size, run each chunk as one batched query, and emit the results as a new stream, in constant memory. The three helpers are:

```scala
package object dynamodb {
  def batchWriteFromStream[R, A, In, B](
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(f: A => DynamoDBQuery[In, B]): ZStream[DynamoDBExecutor with R, Throwable, B]

  def batchReadFromStream[R, A, From: Schema](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(pk: A => KeyConditionExpr.PrimaryKeyExpr[From])
    : ZStream[R with DynamoDBExecutor, Throwable, Either[DynamoDBError.ItemError.DecodingError, (A, Option[From])]]

  def batchReadItemFromStream[R, A](
    tableName: String,
    stream: ZStream[R, Throwable, A],
    mPar: Int = 10
  )(pk: A => PrimaryKey): ZStream[R with DynamoDBExecutor, Throwable, (A, Option[Item])]
}
```

In every helper, `mPar` is the number of batches that run at the same time (default 10). Each batch goes through `DynamoDBQuery.batch`, so the rules and retries described in [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md) apply to it.

## Usage

We use this model in the examples:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }
import zio.stream._

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
  val (id, name) = ProjectionExpression.accessors[Person]
}
```

To load a stream of people into a table and then read some of them back by id:

```scala mdoc:compile-only
val people: ZStream[Any, Throwable, Person] =
  ZStream.range(0, 1000).map(i => Person(i.toString, s"person-$i"))

val load: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(people)(person => DynamoDBQuery.put("people", person)).runDrain

val lookup: ZIO[DynamoDBExecutor, Throwable, Chunk[Either[DynamoDBError.ItemError.DecodingError, (String, Option[Person])]]] =
  batchReadFromStream("people", ZStream("1", "2", "no-such-id"))(id => Person.id.partitionKey === id).runCollect
```

## Writing From a Stream

`batchWriteFromStream` groups the input stream into chunks of up to 25 elements, applies `f` to each element and runs the chunk as one `BatchWriteItem`. The output stream carries the result of each write in input order within a batch. `f` should return a write: a `DynamoDBQuery.put`, `DynamoDBQuery.putItem`, `DynamoDBQuery.deleteFrom` or `DynamoDBQuery.deleteItem`.

To delete a stream of ids:

```scala mdoc:compile-only
val ids: ZStream[Any, Throwable, String] = ZStream("1", "2", "3")

val deleteAll: ZIO[DynamoDBExecutor, Throwable, Unit] =
  batchWriteFromStream(ids, mPar = 4)(id => DynamoDBQuery.deleteFrom[Person]("people")(Person.id.partitionKey === id)).runDrain
```

:::warning
`BatchWriteItem` accepts no condition expressions and no return values other than `ReturnValues.None`. Keep `f` free of `where` conditions and `returns` settings: when a chunk contains more than one such write, the stream fails with `DynamoDBError.BatchError.UnbatchableQueryError`.
:::

## Reading From a Stream

The two read helpers group the input stream into chunks of up to 100 elements and run each chunk as one `BatchGetItem`. They pair every input element with the item found for it, so a missing item is not an error: it shows up as `None`, which gives the effect of a left outer join between the stream and the table.

`batchReadFromStream` is the High Level API version. `pk` builds a primary key expression for each element, and the found items are decoded with the `Schema[From]` in scope. An item that fails to decode becomes a `Left(DecodingError)` element instead of failing the stream:

```scala mdoc:compile-only
val names: ZStream[DynamoDBExecutor, Throwable, String] =
  batchReadFromStream("people", ZStream("1", "2"))(id => Person.id.partitionKey === id).collect {
    case Right((_, Some(person))) => person.name
  }
```

`batchReadItemFromStream` is the Low Level API version. `pk` returns a `PrimaryKey`, and each element is paired with the raw `Item`:

```scala mdoc:compile-only
val items: ZStream[DynamoDBExecutor, Throwable, (String, Option[Item])] =
  batchReadItemFromStream("people", ZStream("1", "2"))(id => PrimaryKey("id" -> id))
```

## Error Handling

All helpers return a stream whose error type is `Throwable`. A failed batch fails the stream with the `DynamoDBError` from the batch, for example a `DynamoDBError.BatchError.WriteError` or `DynamoDBError.BatchError.GetError` that lists the items AWS still left unprocessed after the retries (see [Error Handling](error-handling.md)). To change how often unprocessed items are retried, set a retry policy on the query `f` returns with `DynamoDBQuery#withRetryPolicy`.

## Cats Effect Variants

The `zio-dynamodb-ce` module has the same three helpers for FS2 streams, `batchWriteFromStreamF`, `batchReadFromStreamF` and `batchReadItemFromStreamF`, in `zio.dynamodb.interop.ce.syntax`. They take an `fs2.Stream[F, A]` and an implicit `DynamoDBExceutorF[F]` and return an `fs2.Stream[F, ...]`. See [How to Use ZIO DynamoDB From Cats Effect](../how-to/use-from-cats-effect.md).

For a worked bulk load, see [How to Bulk-Load and Bulk-Read Items With a Stream](../how-to/bulk-load-with-streams.md).
