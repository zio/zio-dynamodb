---
id: cheat-sheet
title: "High Level API Cheat Sheet"
sidebar_label: "Cheat Sheet"
---

This page maps each [AWS DynamoDB API](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html) operation and concept to its ZIO DynamoDB High Level API call. It assumes basic knowledge of DynamoDB.

The examples use this model, with `id` as the partition key and `year` as the sort key:

```scala mdoc:silent
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, year: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]

  val (id, name, year) = ProjectionExpression.accessors[Person]
}
```

```scala mdoc:invisible
// Compiles every snippet used in the tables below
case class Employee(id: String, name: String, year: Int)
object Employee {
  implicit val schema: Schema.CaseClass3[String, String, Int, Employee] = DeriveSchema.gen[Employee]

  val (id, name, year) = ProjectionExpression.accessors[Employee]
}
val id                     = "1"
val listOfIds              = List("1", "2")
val people                 = List(Person("1", "John", 2020))
val oldLastEvaluatedKey    = Option.empty[PrimaryKey]
val cheatSheetCompileCheck = for {
  _                           <- get("personTable")(Person.id.partitionKey === "1").execute
  _                           <- update("personTable")(Person.id.partitionKey === "1")(Person.name.set("Foo")).execute
  _                           <- put("personTable", Person("42", "John", 2020)).execute
  _                           <- deleteFrom("personTable")(Person.id.partitionKey === "1").execute
  _                           <- put("personTable", Person("42", "John", 2020)).where(Person.id === "1").execute
  _                           <- scanAll[Person]("personTable").filter(Person.year > 2020).execute
  _                           <- update("personTable")(Person.id.partitionKey === "1")(Person.name.set("John") + Person.year.add(1)).execute
  _                           <- get("personTable")(Person.id.partitionKey === "1" && Person.year.sortKey === 2020).execute
  _                           <- queryAll[Person]("personTable").whereKey(Person.id.partitionKey === "1" && Person.year.sortKey > 2020).execute
  _                           <- scanAll[Person]("personTable").execute
  _                           <- scanAll[Person]("personTable").parallel(42).execute
  page                        <- scanSome[Person]("personTable", limit = 5).startKey(oldLastEvaluatedKey).execute
  _                           <- queryAll[Person]("personTable").whereKey(Person.id.partitionKey === id).execute
  page2                       <- querySome[Person]("personTable", limit = 5).whereKey(Person.id.partitionKey === "1").execute
  _                           <- batch(listOfIds)(id => get[Person]("personTable")(Person.id.partitionKey === id)).execute
  _                           <- batch(people)(p => put("personTable", p)).execute
  _                           <- (get("personTable")(Person.id.partitionKey === "1") zip get("employeeTable")(Employee.id.partitionKey === "2")).transaction.execute
  _                           <- (put("personTable", Person("1", "John", 2020)) zip put("employeeTable", Employee("2", "Smith", 2024))).transaction.execute
} yield (page, page2)
```

## CRUD Operations

Each single-item operation takes the table name and a primary key built from the model's fields:

| AWS                                                                                               | ZIO DynamoDB                                                                                  |
| ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [GetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html)       | `get("personTable")(Person.id.partitionKey === "1").execute`                                  |
| [UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) | `update("personTable")(Person.id.partitionKey === "1")(Person.name.set("Foo")).execute`       |
| [PutItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html)       | `put("personTable", Person("42", "John", 2020)).execute`                                      |
| [DeleteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html) | `deleteFrom("personTable")(Person.id.partitionKey === "1").execute`                           |

## Expressions

Expressions start from the model's projection expressions. ZIO DynamoDB generates the expression attribute names and values for us:

| AWS                                                                                                                                                  | ZIO DynamoDB                                                                                                                             |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| [Projection Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html)                    | `Person.id`, `Person.name`, `Person.year`                                                                                                |
| [Condition Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)                      | `put("personTable", person).where(Person.id === "1")`                                                                                    |
| Filter Expressions (Scan and Query only)                                                                                                             | `scanAll[Person]("personTable").filter(Person.year > 2020)`                                                                              |
| [Update Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html)                            | `update("personTable")(Person.id.partitionKey === "1")(Person.name.set("John") + Person.year.add(1))`                                    |
| [Primary Keys](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html)                                     | `Person.id.partitionKey === "1"`, or `Person.id.partitionKey === "1" && Person.year.sortKey === 2020` when the table has a sort key      |
| [Key Condition Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression) | `queryAll[Person]("personTable").whereKey(Person.id.partitionKey === "1" && Person.year.sortKey > 2020)`                                 |
| [Expression Attribute Names](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html)            | Managed automatically                                                                                                                    |
| [Expression Attribute Values](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html)          | Managed automatically                                                                                                                    |

## Scan and Query

`scanAll` and `queryAll` return a ZIO `Stream` of items; `scanSome` and `querySome` return one page, a `(Chunk[Person], LastEvaluatedKey)` pair whose second element is the key to continue from:

| AWS                                                                                                        | ZIO DynamoDB                                                                                                            |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| [Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)                      | `stream <- scanAll[Person]("personTable").execute`                                                                      |
| [Parallel Scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan) | `stream <- scanAll[Person]("personTable").parallel(42).execute`                                                     |
| [Scan with paging](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)          | `page <- scanSome[Person]("personTable", limit = 5).startKey(oldLastEvaluatedKey).execute`                              |
| [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)                    | `stream <- queryAll[Person]("personTable").whereKey(Person.id.partitionKey === id).execute`                             |
| [Query with paging](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)        | `page <- querySome[Person]("personTable", limit = 5).whereKey(Person.id.partitionKey === "1").execute`                  |

## Batching and Transactions

`DynamoDBQuery.batch` and `DynamoDBQuery#zip` group queries into batch requests, and `DynamoDBQuery#transaction` runs zipped queries atomically. Transactions can span several tables:

| AWS                                                                                                                | ZIO DynamoDB                                                                                                                                       |
| ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| [BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html)               | `people <- batch(listOfIds)(id => get[Person]("personTable")(Person.id.partitionKey === id)).execute`                                              |
| [BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)           | `_ <- batch(people)(p => put("personTable", p)).execute`                                                                                          |
| [TransactGetItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html)       | `tuple <- (get("personTable")(Person.id.partitionKey === "1") zip get("employeeTable")(Employee.id.partitionKey === "2")).transaction.execute`     |
| [TransactWriteItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)   | `_ <- (put("personTable", Person("1", "John", 2020)) zip put("employeeTable", Employee("2", "Smith", 2024))).transaction.execute`                   |

For complete examples, see the High Level API integration tests: [CRUD](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiCrudSpec.scala), [mapping](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiMappingSpec.scala), [scan and query](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeScanAndQuerySpec.scala) and [streaming](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeStreamingUtilsSpec.scala).
