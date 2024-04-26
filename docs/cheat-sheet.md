# High Level API Cheat Sheet

Note this guide assumes the reader has a basic knowledge of [AWS DynamoDB API](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html).

Assuming the below model
```scala
final case class Person(id: String, name: String, year: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]
  val (id, name, year) = ProjectionExpression.accessors[Person]
}
```


| AWS                           | ZIO DynamoDB |
|-------------------------------| --- |
| [GetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html)                       | `person <- get("personTable")(Person.id.partitionKey === "1").execute` |
| [UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)                    | `_ <- update("personTable")(Person.id.partitionKey === "1")(Person.name.set("Foo")).execute` |
| [PutItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html)                       | _ <- `put("personTable", Person(42, "John", 2020)).execute` |
| [DeleteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html)                    | `_ <- deleteFrom("personTable")(Person.id.partitionKey === "1").execute` |
|                               | |
| [Projection Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html)        | `Person.name`  |
| [Condition Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)         | `<DynamoDBQuery>.where(Person.id === "1")` |
| **Filter Expressions** apply to Scan and Query | `<DynamoDBQuery>.filter(Person.year > 2020)`   |
| [Update Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-UpdateExpression)            | `update("personTable")(Person.id.partitionKey === "1")(Person.name.set("John") + Person.year.add(1))` |
| [Primary Keys](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html)                  | `Person.id.partitionKey === "1"` or `Person.id.partitionKey === "1" && Person.year.sortKey === 2020` if table has both partition _and_ sort keys |
| [Key Condition Expressions](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression)     | `<query>.whereKey(Person.id.partitionKey === "1" && Person.year.sortKey > 2020)` |
| [_Expression Attribute Names_](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html)  | _Managed automatically!_ |
| [_Expression Attribute Values_](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html) | _Managed automatically!_ |
|                               | |
| [Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)                          |	`stream <- scanAll[Person]("personTable").execute`
| [Scan with parallel processing](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html) |	`stream <- scanAll[Person]("personTable").parallel(42).execute`
| [Scan with paging](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)              |	`(people, lastEvaluatedKey) <- scanSome[Person](tableName, limit = 5).startKey(oldLastEvaluatedKey).execute`
| [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)                         |	`stream <- queryAll[Person]("personTable").whereKey(Person.name.contains("mi")).execute`
| [Query with paging](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)             |	`(people, lastEvaluatedKey) <- querySome[Person](tableName, limit = 5).whereKey(Person.name.contains("mi"))`
| | |
| [BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html) | `people <- DynamoDBQuery.forEach(listOfIds)(id => DynamoDBQuery.get[Person]("personTable")(Person.id.partitionKey === id)).execute`|
| [BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html) | _ <- `DynamoDBQuery.forEach(people)(p => put("personTable", p)).execute` |
| | |
| [TransactGetItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html) | `people <- DynamoDBQuery.forEach(listOfIds)(p => DynamoDBQuery.get[Person]("personTable")(Person.id.partitionKey === p.id)).transaction.execute` |
| [TransactWriteItems](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html) | `_ <- DynamoDBQuery.forEach(listOfIds){ p => DynamoDBQuery.forEach(people)(p => put("personTable", p)).transaction.execute` |

