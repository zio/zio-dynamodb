---
id: auto-batching-and-parallelisation
title: "Auto Batching and Parallelisation"
---

When we combine `DynamoDBQuery` values with `DynamoDBQuery#zip` (or its variants), `DynamoDBQuery#execute` groups the parts DynamoDB can batch into `BatchGetItem` and `BatchWriteItem` requests, and runs every other part in parallel. When we combine them with `DynamoDBQuery.batch`, the parts are batched too, but nothing runs in parallel: when more than one part cannot be batched, the query fails.

The examples on this page use this model:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]

  val (id, name, age) = ProjectionExpression.accessors[Person]
}

val people = List(Person("1", "John", 21), Person("2", "Jane", 22))
```

Both queries below send a single `BatchWriteItem` request with two `PutItem`s:

```scala mdoc:silent
val zippedWrites = put("person", Person("1", "John", 21)).zip(put("person", Person("2", "Jane", 22)))

val batchedWrites = DynamoDBQuery.batch(people)(person => put("person", person))
```

## Batching Rules

DynamoDB's [BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) accepts only puts and deletes without condition expressions, and [BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html) accepts only gets. ZIO DynamoDB follows those limits. When a composite query runs, its parts are grouped into gets and writes, and a part joins a batch only if:

- It is a `PutItem` or `DeleteItem` (`put` or `deleteFrom` in the High Level API) without a condition expression, whose `returns` is `ReturnValues.None` (the default).
- It is a `GetItem` (`get` in the High Level API) whose projections include the primary key, so that the response can be matched to the request. All attributes are returned by default, so this matters only when we set projections ourselves.
- There is more than one part of its kind: a single get or a single write is sent as an ordinary request.

For queries combined with `DynamoDBQuery#zip`, a part that does not qualify runs in parallel with the batches. For queries built with `DynamoDBQuery.batch`, when more than one part does not qualify, `DynamoDBQuery#execute` fails with `DynamoDBError.BatchError.UnbatchableQueryError`, whose `reasons` list how each part was classified, including the rules that were broken.

## Batching With batch

`DynamoDBQuery.batch(values)(f)` is the recommended way to batch: it zips one query per element and keeps the results in the order of `values`. We still have to keep each batch within DynamoDB's limits: `BatchWriteItem` takes at most **25** puts or deletes, and `BatchGetItem` takes at most **100** gets.

A larger batch fails with an AWS error. The [streaming batch helpers](streaming-batch-helpers.md) split a stream into batches of the right size for us.

## Automatic Retry of Unprocessed Items

`BatchWriteItem` and `BatchGetItem` can return a list of unprocessed items or keys, for example when the table is throttled. ZIO DynamoDB resends them automatically. If items are still unprocessed after the retries, the query fails with `DynamoDBError.BatchError.WriteError` or `DynamoDBError.BatchError.GetError`, which list them (see [Error Handling](error-handling.md#batch-errors)).

The default retry policy retries three times with jittered exponential backoff starting at 50 milliseconds:

```scala
Schedule.recurs(3) && Schedule.exponential(50.milliseconds).jittered(0, 10)
```

`DynamoDBQuery#withRetryPolicy` replaces it for one query:

```scala mdoc:compile-only
val retryLonger: Schedule[Any, Throwable, Any] =
  Schedule.recurs(10) && Schedule.exponential(100.milliseconds).jittered

val program: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[Option[Person]]] =
  batchedWrites.withRetryPolicy(retryLonger).execute
```

## Integrating Batching With ZIO Streams

To write or read a large or unbounded number of items, the helpers `batchWriteFromStream`, `batchReadFromStream` and `batchReadItemFromStream` in the `zio.dynamodb` package read a `ZStream`, group it into batches of the maximum size and run them with bounded parallelism. See [Streaming Batch Helpers](streaming-batch-helpers.md) and [How to Bulk-Load and Bulk-Read Items With a Stream](../how-to/bulk-load-with-streams.md).
