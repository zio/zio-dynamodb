---
id: scan-some
title: "scanSome"
---

`DynamoDBQuery.scanSome` reads one page of at most `limit` items from a table and returns them with the key to continue from:

```scala
object DynamoDBQuery {
  def scanSome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]
}
```

`LastEvaluatedKey` is an `Option[PrimaryKey]`: `Some(key)` when more items may follow, `None` when the scan reached the end of the table. Passing the key to `DynamoDBQuery#startKey` reads the next page. Paging keeps memory bounded, for example behind a REST endpoint that serves one page per request.

The examples use the equipment model from [Scan and Query Operations](index.md):

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Equipment(id: String, year: Int, name: String, price: Double)
object Equipment {
  implicit val schema: Schema.CaseClass4[String, Int, String, Double, Equipment] = DeriveSchema.gen[Equipment]

  val (id, year, name, price) = ProjectionExpression.accessors[Equipment]
}
```

To read two pages of two items:

```scala mdoc:compile-only
val twoPages: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[Equipment]] =
  for {
    page1 <- DynamoDBQuery.scanSome[Equipment]("equipment", limit = 2).execute
    page2 <- DynamoDBQuery.scanSome[Equipment]("equipment", limit = 2).startKey(page1._2).execute
  } yield page1._1 ++ page2._1
```

With a filter, DynamoDB applies `limit` before filtering, so a page can hold fewer than `limit` items, or none, even when more items follow; keep reading until the key is `None`. [How to Page Through Scan and Query Results](../../../how-to/page-through-results.md) shows a complete loop.

## Options

These options apply to `scanSome`:

```scala
<SCAN_SOME_QUERY>
  .startKey(<LastEvaluatedKey>)    // continue from a previous page
  .filter(<ConditionExpression>)   // e.g. Equipment.price > 1.0
  .indexName(<IndexName>)          // scan a secondary index
  .consistency(<ConsistencyMode>)
  .selectAllAttributes | .selectAllProjectedAttributes | .selectSpecificAttributes | .selectCount
  .capacity(<ReturnConsumedCapacity>)
```
