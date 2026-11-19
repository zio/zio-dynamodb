---
id: query-some
title: "querySome"
---

`DynamoDBQuery.querySome` reads one page of at most `limit` items with one partition key and returns them with the key to continue from:

```scala
object DynamoDBQuery {
  def querySome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]
}
```

Like [queryAll](query-all.md), it needs a key condition set with `DynamoDBQuery#whereKey`. Like [scanSome](scan-some.md), it returns a `LastEvaluatedKey` that is `Some(key)` when more items may follow and `None` at the end, and `DynamoDBQuery#startKey` continues from it.

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

To read partition `"1"` two items at a time:

```scala mdoc:compile-only
val firstTwoPages: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[Equipment]] = {
  val query = DynamoDBQuery.querySome[Equipment]("equipment", limit = 2).whereKey(Equipment.id.partitionKey === "1")

  for {
    page1 <- query.execute
    page2 <- query.startKey(page1._2).execute
  } yield page1._1 ++ page2._1
}
```

## Options

These options apply to `querySome`:

```scala
<QUERY_SOME_QUERY>
  .whereKey(<KeyConditionExpr>)    // required
  .startKey(<LastEvaluatedKey>)    // continue from a previous page
  .filter(<ConditionExpression>)   // applied after the key condition and the limit
  .sortOrder(<ascending>)          // true (default) or false
  .indexName(<IndexName>)          // query a secondary index
  .consistency(<ConsistencyMode>)
  .selectAllAttributes | .selectAllProjectedAttributes | .selectSpecificAttributes | .selectCount
  .capacity(<ReturnConsumedCapacity>)
```
