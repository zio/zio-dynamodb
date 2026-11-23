---
id: index
title: "Low Level API"
sidebar_label: "Low Level API"
---

You rarely need the Low Level API directly; prefer the [High Level API](../high-level-api/index.md) unless you work with items whose shape is not known at compile time. The Low Level API constructors live in the `DynamoDBQuery` companion object. Their names contain `Item` because they work directly on DynamoDB [items](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html), represented as an `AttrMap` (see [AttributeValue and AttrMap](attribute-value.md)), and their expressions are built with untyped projection expressions from [`$`](dollar-function.md):

```scala
object DynamoDBQuery {

  // CRUD operations
  def getItem(tableName: String, key: PrimaryKey, projections: ProjectionExpression[_, _]*): DynamoDBQuery[Any, Option[Item]]
  def putItem(tableName: String, item: Item): DynamoDBQuery[Any, Option[Item]]
  def updateItem[A](tableName: String, key: PrimaryKey)(action: Action[A]): DynamoDBQuery[A, Option[Item]]
  def deleteItem(tableName: String, key: PrimaryKey): Write[Any, Option[Item]]

  // Scan and query operations
  def scanSomeItem(tableName: String, limit: Int, projections: ProjectionExpression[_, _]*): DynamoDBQuery[Any, (Chunk[Item], LastEvaluatedKey)]
  def scanAllItem(tableName: String, projections: ProjectionExpression[_, _]*): DynamoDBQuery[Any, Stream[Throwable, Item]]
  def querySomeItem(tableName: String, limit: Int, projections: ProjectionExpression[_, _]*): DynamoDBQuery[Any, (Chunk[Item], LastEvaluatedKey)]
  def queryAllItem(tableName: String, projections: ProjectionExpression[_, _]*): DynamoDBQuery[Any, Stream[Throwable, Item]]
}
```

`DynamoDBQuery.getItem` returns `None` when no item has the key. `DynamoDBQuery.scanSomeItem` and `DynamoDBQuery.querySomeItem` return one page of at most `limit` items and the key to continue from; `DynamoDBQuery.scanAllItem` and `DynamoDBQuery.queryAllItem` return a stream of every item. With no projections, every attribute is returned.

The program below uses each kind of operation, with expressions built from `$`:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._
import zio.dynamodb.ProjectionExpression.$

val table = "person"

val program: ZIO[DynamoDBExecutor, DynamoDBError, Chunk[Item]] =
  for {
    _      <- DynamoDBQuery.putItem(table, Item("id" -> "1", "name" -> "John", "age" -> 42)).where($("id").notExists).execute
    found  <- DynamoDBQuery.getItem(table, PrimaryKey("id" -> "1"), $("name")).execute
    _      <- ZIO.debug(found)
    _      <- DynamoDBQuery.updateItem(table, PrimaryKey("id" -> "1"))($("age").add(1)).execute
    stream <- DynamoDBQuery.queryAllItem(table).whereKey($("id").partitionKey === "1").execute
    items  <- stream.runCollect.mapError(DynamoDBError.UnknownNonFatalError(_))
    firstPage <- DynamoDBQuery.scanSomeItem(table, limit = 10).filter($("age") > 40).execute
    _      <- DynamoDBQuery.deleteItem(table, PrimaryKey("id" -> "1")).execute
  } yield items ++ firstPage._1
```
