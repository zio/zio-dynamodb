---
id: low-level-api
title: "Low Level API"
---

The Low Level API maps one to one onto DynamoDB's own abstractions while still removing most of the boilerplate of the AWS SDK. Its surface area is:

- `AttrMap`, a container for DynamoDB attributes that converts Scala values to `AttributeValue`s automatically. Its type aliases `Item` and `PrimaryKey` name the two ways it is used (see [AttributeValue and AttrMap](../reference/low-level-api/attribute-value.md)).
- The `$` function, which creates an untyped `ProjectionExpression` from a path string such as `$("person.address.houseNumber")` (see [$ and parse Functions](../reference/low-level-api/dollar-function.md)).
- The query constructors in the `DynamoDBQuery` companion object whose names contain `Item`: `getItem`, `putItem`, `updateItem`, `deleteItem`, `scanAllItem`, `scanSomeItem`, `queryAllItem` and `querySomeItem`.
- Expressions built from a `ProjectionExpression`: condition expressions such as `$("name") === "John"`, update expressions such as `$("name").set("Smith")`, and key expressions such as `$("id").partitionKey === "1"`.

The program below writes, reads, updates and deletes an item with the Low Level API:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._
import zio.dynamodb.ProjectionExpression.$

val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    _          <- DynamoDBQuery.putItem("person-table", Item("id" -> "1", "name" -> "John", "age" -> 42)).execute
    maybeFound <- DynamoDBQuery.getItem("person-table", PrimaryKey("id" -> "1")).execute
    _          <- ZIO.debug(s"found: $maybeFound")
    _          <- DynamoDBQuery.updateItem("person-table", PrimaryKey("id" -> "1"))(
                    $("name").set("Smith") + $("age").set(21)
                  ).execute
    _          <- DynamoDBQuery.deleteItem("person-table", PrimaryKey("id" -> "1")).execute
  } yield ()
```

The Low Level API has two drawbacks compared with the [High Level API](high-level-api.md):

- It is not type-safe: a typo in a field name, or a comparison with a value of the wrong type, compiles and fails at runtime.
- We encode and decode our case classes ourselves, which is usually the biggest part of the work.
