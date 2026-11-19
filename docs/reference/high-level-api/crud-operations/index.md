---
id: index
title: "CRUD Operations"
sidebar_label: "CRUD Operations"
---

The High Level API mirrors DynamoDB's single-item operations with type-safe versions. They work on a Scala model of the table: a case class with an implicit schema in its companion object, and projection expressions for its fields generated with `ProjectionExpression.accessors`:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]

  val (id, name, age) = ProjectionExpression.accessors[Person]
}
```

The four operations are:

```scala
object DynamoDBQuery {
  def get[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[From, Either[ItemError, From]]
  def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]]
  def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(action: Action[From]): DynamoDBQuery[From, Option[From]]
  def deleteFrom[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[Any, Option[From]]
}
```

Each one has its own page: [put](put.md), [get](get.md), [update](update.md) and [deleteFrom](delete.md). They build a query; nothing reaches DynamoDB until we call `DynamoDBQuery#execute`:

```scala mdoc:compile-only
import zio._

val program: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  for {
    _       <- DynamoDBQuery.put("person", Person("1", "John", 21)).execute
    _       <- DynamoDBQuery.update("person")(Person.id.partitionKey === "1")(Person.age.add(1)).execute
    found   <- DynamoDBQuery.get("person")(Person.id.partitionKey === "1").execute.absolve
    _       <- ZIO.debug(found)
    deleted <- DynamoDBQuery.deleteFrom("person")(Person.id.partitionKey === "1").returns(ReturnValues.AllOld).execute
  } yield deleted
```
