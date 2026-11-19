---
id: error-handling
title: "Error Handling"
---

`DynamoDBError` is the sealed trait of every error that `DynamoDBQuery#execute` can fail with:

```scala
sealed trait DynamoDBQuery[-In, +Out] {
  def execute: ZIO[DynamoDBExecutor, DynamoDBError, Out]
}
```

`DynamoDBError` extends `Throwable` (without a stack trace) and has a `message`. Its hierarchy is:

```
DynamoDBError
├── ItemError
│   ├── ValueNotFound(message)
│   └── DecodingError(message)
├── AWSError(cause: DynamoDbException)
├── BatchError
│   ├── GetError(unprocessedKeys: Map[String, Set[PrimaryKey]])
│   ├── WriteError(unprocessedItems: Map[String, Chunk[BatchError.Write]])
│   └── UnbatchableQueryError(reasons: Set[String])
├── TransactionError
│   ├── EmptyTransaction
│   ├── MixedTransactionTypes
│   └── InvalidTransactionActions(invalidActions)
└── UnknownNonFatalError(cause: Throwable)
```

## Item Errors

`DynamoDBError.ItemError` covers problems with a single item in the High Level API:

- `ItemError.ValueNotFound` is the `Left` that `DynamoDBQuery.get` returns when no item has the given key. To treat a missing item as `None` instead, use the `maybeFound` extension method; see [Working With get Return Values](high-level-api/crud-operations/get.md#working-with-get-return-values).
- `ItemError.DecodingError` is returned when stored data does not match the model's schema, for example a missing required field or a string where the schema expects a number.

## AWS Errors

`DynamoDBError.AWSError` wraps the `DynamoDbException` thrown by the AWS SDK. We pattern match on its `cause` when we expect a specific AWS error, such as the `ConditionalCheckFailedException` of a write whose condition is false:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]

  val (id, name) = ProjectionExpression.accessors[Person]
}

// Inserts the person, or returns false if a person with that id already exists
def insert(person: Person): ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  DynamoDBQuery
    .put("person", person)
    .where(Person.id.notExists)
    .execute
    .as(true)
    .catchSome { case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => ZIO.succeed(false) }
```

[How to Handle a Failed Conditional Write](../how-to/handle-failed-conditional-writes.md) covers this case in depth.

## Batch Errors

`DynamoDBError.BatchError` matters when queries are batched, through `DynamoDBQuery.batch`, through `zip`, or through the [streaming batch helpers](streaming-batch-helpers.md). By the time a batch error is raised, the automatic retries have already run. In a long-running process, a typical handler records the failed items and carries on.

- `BatchError.GetError` lists, per table name, the primary keys DynamoDB did not process.
- `BatchError.WriteError` lists, per table name, the writes DynamoDB did not process, each a `BatchError.Put(item)` or a `BatchError.Delete(key)`.
- `BatchError.UnbatchableQueryError` is returned when a query built with `DynamoDBQuery.batch` cannot be batched; `reasons` says why.

See [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md) for the rules.

## Transaction Errors

`DynamoDBError.TransactionError` is returned by `DynamoDBQuery#transaction` at execution time, or by `DynamoDBQuery#safeTransaction` before execution, when the query is not a valid transaction:

- `TransactionError.EmptyTransaction`: the transaction contains no actions.
- `TransactionError.MixedTransactionTypes`: the transaction mixes reads and writes.
- `TransactionError.InvalidTransactionActions`: the transaction contains actions DynamoDB transactions do not support, such as a scan or a table operation.

See [How to Run Operations in a Transaction](../how-to/run-operations-in-a-transaction.md).

## Unknown Errors

`DynamoDBError.UnknownNonFatalError` wraps any other non-fatal exception (as defined by `scala.util.control.NonFatal`) raised while running a query.
