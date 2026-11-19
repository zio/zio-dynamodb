---
id: index
title: "Scan and Query Operations"
sidebar_label: "Scan and Query Operations"
---

The High Level API mirrors DynamoDB's [Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html) and [Query](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html) operations. A scan reads every item of a table or index; a query reads only the items with one partition key, optionally narrowed by a sort key condition. Each comes in two forms:

| Operation   | Returns                                     | Use when                                                            |
| ----------- | ------------------------------------------- | ------------------------------------------------------------------- |
| `scanAll`   | `Stream[Throwable, A]` of every item        | Processing a whole table as a stream.                               |
| `scanSome`  | One page: `(Chunk[A], LastEvaluatedKey)`    | Paging through a table, e.g. behind a paged REST endpoint.          |
| `queryAll`  | `Stream[Throwable, A]` of matching items    | Processing every item of a partition as a stream.                   |
| `querySome` | One page: `(Chunk[A], LastEvaluatedKey)`    | Paging through a partition.                                         |

```scala
object DynamoDBQuery {
  def scanAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
  def scanSome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]
  def queryAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
  def querySome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]
}
```

The `All` forms fetch the pages lazily as the stream is consumed. An item that cannot be decoded fails the stream with an `IllegalStateException`. The `Some` forms fail the query with a `DynamoDBError.ItemError.DecodingError` instead.

The pages of this section use a table of equipment whose partition key is `id` and whose sort key is `year`:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Equipment(id: String, year: Int, name: String, price: Double)
object Equipment {
  implicit val schema: Schema.CaseClass4[String, Int, String, Double, Equipment] = DeriveSchema.gen[Equipment]

  val (id, year, name, price) = ProjectionExpression.accessors[Equipment]
}
```

See [scanAll](scan-all.md), [scanSome](scan-some.md), [queryAll](query-all.md) and [querySome](query-some.md), and [How to Page Through Scan and Query Results](../../../how-to/page-through-results.md) for a complete paging loop.
