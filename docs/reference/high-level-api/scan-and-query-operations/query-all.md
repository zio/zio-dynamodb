---
id: query-all
title: "queryAll"
---

`DynamoDBQuery.queryAll` reads every item with one partition key, optionally narrowed by a sort key condition, and returns them as a ZIO `Stream`, ordered by sort key:

```scala
object DynamoDBQuery {
  def queryAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
}
```

A query always needs a key condition, set with `DynamoDBQuery#whereKey`: an equality on the partition key, optionally combined with `&&` and a condition on the sort key (see [Primary Keys](../primary-keys.md)). The examples use the equipment model from [Scan and Query Operations](index.md), with `id` as partition key and `year` as sort key:

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

To read the items of partition `"1"` from after 2020, newest first:

```scala mdoc:compile-only
val recent: ZIO[DynamoDBExecutor, Throwable, Chunk[Equipment]] =
  for {
    stream <- DynamoDBQuery
                .queryAll[Equipment]("equipment")
                .whereKey(Equipment.id.partitionKey === "1" && Equipment.year.sortKey > 2020)
                .sortOrder(ascending = false)
                .execute
    items  <- stream.runCollect
  } yield items
```

## Options

These options apply to `DynamoDBQuery.queryAll`:

```scala
<QUERY_ALL_QUERY>
  .whereKey(<KeyConditionExpr>)    // required, e.g. Equipment.id.partitionKey === "1" && Equipment.year.sortKey > 2020
  .filter(<ConditionExpression>)   // e.g. Equipment.price > 1.0, applied after the key condition
  .sortOrder(<ascending>)          // true (default) or false
  .indexName(<IndexName>)          // query a secondary index
  .consistency(<ConsistencyMode>)
  .startKey(<LastEvaluatedKey>)
  .selectAllAttributes | .selectAllProjectedAttributes | .selectSpecificAttributes | .selectCount
  .capacity(<ReturnConsumedCapacity>)
```

To query a secondary index, pass its name to `DynamoDBQuery#indexName` and use the index's key attributes in `DynamoDBQuery#whereKey`. Creating indexes is covered in [Table Management](../../table-management.md).
