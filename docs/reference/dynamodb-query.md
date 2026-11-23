---
id: dynamodb-query
title: "DynamoDBQuery"
---

`DynamoDBQuery[-In, +Out]` is a description of one or more DynamoDB requests that produces an `Out` when run. Both the High Level and the Low Level API constructors return it, so queries from both APIs combine freely. `In` is the model type a High Level query works on (`Any` for Low Level queries); it lets the compiler reject, for example, a filter on `Person` fields attached to a scan of `Order`s.

## Running a Query

`DynamoDBQuery#execute` turns the description into a ZIO effect:

```scala
sealed trait DynamoDBQuery[-In, +Out] {
  def execute: ZIO[DynamoDBExecutor, DynamoDBError, Out]
}
```

The effect needs a `DynamoDBExecutor` in its environment (see [DynamoDBExecutor](dynamodb-executor.md)) and fails with a [`DynamoDBError`](error-handling.md). When we run a query, `DynamoDBQuery#execute` splits a composite query (built with `DynamoDBQuery#zip` or `DynamoDBQuery.batch`) into its parts, then batches the parts that DynamoDB can batch and runs the others in parallel; see [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md) for the rules. It then hands each request to the `DynamoDBExecutor`, which converts it to a ZIO AWS request, sends it, and converts the response back to an `Item`.

For High Level API queries, the codecs derived from the model's `Schema` convert between the model and the `Item`.

## Constructors

The `DynamoDBQuery` companion object holds every constructor. They fall into these groups:

| Group                     | High Level API                                       | Low Level API                                                  |
| ------------------------- | ---------------------------------------------------- | -------------------------------------------------------------- |
| Single item               | `get`, `put`, `update`, `deleteFrom`                 | `getItem`, `putItem`, `updateItem`, `deleteItem`               |
| Sum types (experimental)  | `getWithNarrow`, `putWithNarrow`                     |                                                                |
| Scan and query            | `scanAll`, `scanSome`, `queryAll`, `querySome`       | `scanAllItem`, `scanSomeItem`, `queryAllItem`, `querySomeItem` |
| Transactions              | `conditionCheck`                                     | `conditionCheck`                                               |
| Tables                    | `createTable`, `deleteTable`, `describeTable`        | `createTable`, `deleteTable`, `describeTable`                  |
| Batching                  | `batch`                                              | `batch`                                                        |
| Pure values               | `succeed`, `fail`, `fromEither`, `apply`             | `succeed`, `fail`, `fromEither`, `apply`                       |

The [High Level API](high-level-api/index.md) and [Low Level API](low-level-api/index.md) pages document the item constructors, [Table Management](table-management.md) the table constructors, and [How to Run Operations in a Transaction](../how-to/run-operations-in-a-transaction.md) `conditionCheck`.

### Batching Many Queries

`DynamoDBQuery.batch` builds one query per element of a collection and zips them together:

```scala
object DynamoDBQuery {
  def batch[In, A, B](values: Iterable[A])(body: A => DynamoDBQuery[In, B]): DynamoDBQuery[In, Chunk[B]]
}
```

The results come back in the order of `values`. When more than one of the zipped queries cannot be batched, `DynamoDBQuery#execute` fails with `DynamoDBError.BatchError.UnbatchableQueryError` instead of running them in parallel. Unprocessed items or keys are retried, and a `BatchError.WriteError` or `BatchError.GetError` lists the ones that still failed. See [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md).

### Pure Values

`DynamoDBQuery.succeed(a)` (also `DynamoDBQuery(a)`) and `DynamoDBQuery.fail(error)` lift a value or a `DynamoDBError` into a query without calling DynamoDB, and `DynamoDBQuery.fromEither` lifts an `Either[DynamoDBError.ItemError, A]`. They are useful as the neutral element when folding a list of queries:

```scala mdoc:compile-only
import zio.dynamodb._

val noWrites: DynamoDBQuery[Any, Unit] = DynamoDBQuery.succeed(())

val writes: DynamoDBQuery[Any, Unit] =
  List("1", "2").foldLeft(noWrites) { (acc, id) =>
    acc.zipLeft(DynamoDBQuery.deleteItem("person", PrimaryKey("id" -> id)))
  }
```

## Combinators

These combinators compose queries. Queries combined with `DynamoDBQuery#zip` and its variants are eligible for automatic batching and parallel execution:

| Combinator                       | Alias  | Description                                                                         |
| -------------------------------- | ------ | ----------------------------------------------------------------------------------- |
| `DynamoDBQuery#map`              |        | Transforms the result of a query with a function.                                   |
| `DynamoDBQuery#zip`              | `<*>`  | Combines two queries and returns both results as a tuple.                           |
| `DynamoDBQuery#zipWith`          |        | Zips two queries and combines their results with a function.                       |
| `DynamoDBQuery#zipLeft`          | `<*`   | Zips two queries and keeps the result of the left one.                              |
| `DynamoDBQuery#zipRight`         | `*>`   | Zips two queries and keeps the result of the right one.                             |
| `DynamoDBQuery#transaction`      |        | Runs the query as one DynamoDB transaction; invalid actions fail at execution time. |
| `DynamoDBQuery#safeTransaction`  |        | Like `transaction`, but returns `Either[DynamoDBError.TransactionError, DynamoDBQuery[Any, Out]]` so invalid actions are reported before execution. |

## Request Options

These methods set options on the request. Each one applies to the query types listed and leaves other query types unchanged, so applying an option to a query type that does not support it has no effect:

| Method                                  | Applies to                                       | Description                                                                                                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `capacity(ReturnConsumedCapacity)`      | Item, scan, query, batch and transaction requests | Sets [ReturnConsumedCapacity](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-ReturnConsumedCapacity): `Indexes`, `Total` or `None` (default). The capacity data in the response is not returned to us. |
| `consistency(ConsistencyMode)`          | `get`, scans and queries                         | Sets [ConsistentRead](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-ConsistentRead): `ConsistencyMode.Strong` or `ConsistencyMode.Weak` (default).                                     |
| `filter(ConditionExpression)`           | Scans and queries                                | Sets the filter expression. DynamoDB applies it after reading, so it saves network traffic but not read capacity. See [Condition and Filter Expressions](condition-expressions.md).                                                       |
| `indexName(String)`                     | Scans and queries                                | Reads from a local or global secondary index ([IndexName](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-IndexName)).                                                                     |
| `gsi(...)`, `lsi(...)`                  | `createTable`                                    | Adds a global or local secondary index to the table being created. See [Table Management](table-management.md).                                                                                                                           |
| `metrics(ReturnItemCollectionMetrics)`  | `put`, `update`, `deleteFrom`, transactions      | Sets [ReturnItemCollectionMetrics](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ReturnItemCollectionMetrics): `Size` or `None` (default). The metrics are not returned to us.         |
| `parallel(n)`                           | `scanAll`                                        | Runs a [parallel scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan) with `n` segments and merges the items into one stream, in no particular order.                                       |
| `returns(ReturnValues)`                 | `put`, `update`, `deleteFrom`                    | Sets [ReturnValues](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-ReturnValues): `None` (default), `AllOld`, `UpdatedOld`, `AllNew` or `UpdatedNew`. See the [CRUD Operations](high-level-api/crud-operations/index.md) pages for which values each operation accepts. |
| `selectAllAttributes`, `selectAllProjectedAttributes`, `selectSpecificAttributes`, `selectCount` | Scans and queries | Sets [Select](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select), which attributes a scan or query returns.                                                    |
| `sortOrder(ascending: Boolean)`         | Queries                                          | Sets [ScanIndexForward](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward); `true` (default) sorts by sort key ascending.                                                 |
| `startKey(LastEvaluatedKey)`            | Scans and queries                                | Sets [ExclusiveStartKey](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ExclusiveStartKey), the key to continue from. See [How to Page Through Scan and Query Results](../how-to/page-through-results.md). |
| `where(ConditionExpression)`            | `put`, `update`, `deleteFrom`                    | Sets the [condition expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html); the write fails when the condition is false.                                                     |
| `whereKey(KeyConditionExpr)`            | Queries                                          | Sets the [key condition expression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression). A query needs one. See [Primary Keys](high-level-api/primary-keys.md).       |
| `withClientRequestToken(String)`        | Write transactions                               | Sets the [ClientRequestToken](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken) that makes a write transaction idempotent.                     |
| `withRetryPolicy(Schedule)`             | `get`, `put`, `deleteFrom` (when batched)        | Replaces the retry policy for unprocessed items of the batch request the query ends up in. See [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md#automatic-retry-of-unprocessed-items).                                                                 |

On a composite query, each option is applied to every query inside it.
