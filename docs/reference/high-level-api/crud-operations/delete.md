---
id: delete
title: "deleteFrom"
---

`DynamoDBQuery.deleteFrom` deletes the item with the given primary key:

```scala
object DynamoDBQuery {
  def deleteFrom[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[Any, Option[From]]
}
```

We build the primary key expression from the model's projection expressions:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]

  val (id, name, age) = ProjectionExpression.accessors[Person]
}
```

Deleting an item that does not exist succeeds:

```scala mdoc:compile-only
val delete: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery.deleteFrom("person")(Person.id.partitionKey === "1").execute
```

## Options

These options apply to `DynamoDBQuery.deleteFrom`:

```scala
<DELETE_QUERY>
  .returns(<ReturnValues>)       // ReturnValues.AllOld | ReturnValues.None (default)
  .where(<ConditionExpression>)
  .metrics(<ReturnItemCollectionMetrics>)
  .capacity(<ReturnConsumedCapacity>)
```

With `DynamoDBQuery#returns(ReturnValues.AllOld)`, the result holds the deleted item; with a condition, the delete fails instead of deleting an item that does not match:

```scala mdoc:compile-only
val deleteMinor: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery
    .deleteFrom("person")(Person.id.partitionKey === "1")
    .where(Person.age < 18)
    .returns(ReturnValues.AllOld)
    .execute
```
