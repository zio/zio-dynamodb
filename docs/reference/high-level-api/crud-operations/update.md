---
id: update
title: "update"
---

`DynamoDBQuery.update` changes attributes of the item with the given primary key. DynamoDB creates the item if it does not exist yet:

```scala
object DynamoDBQuery {
  def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(action: Action[From]): DynamoDBQuery[From, Option[From]]
}
```

Both the primary key expression and the update action are built from the model's projection expressions. The examples use this model:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int, nickNames: List[String], tags: Set[String], address: Option[String])
object Person {
  implicit val schema: Schema.CaseClass6[String, String, Int, List[String], Set[String], Option[String], Person] =
    DeriveSchema.gen[Person]

  val (id, name, age, nickNames, tags, address) = ProjectionExpression.accessors[Person]
}
```

Actions are combined with `+` and applied in one request:

```scala mdoc:compile-only
val renameAndAge: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery
    .update("person")(Person.id.partitionKey === "1")(
      Person.name.set("John") + Person.age.add(1)
    )
    .execute
```

## Update Actions

The [update expression](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html) actions are methods on a projection expression:

| Action                         | Description                                                                                                  |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `a + b`                        | Combines two actions into one update.                                                                        |
| `set(value)`                   | Sets the attribute to a value.                                                                               |
| `set(otherPath)`               | Sets the attribute to the value of another attribute of the same item.                                       |
| `setIfNotExists(value)`        | Sets the attribute only if it does not exist yet.                                                            |
| `append(element)`              | Adds an element to the end of a list attribute.                                                              |
| `appendList(elements)`         | Adds a list of elements to the end of a list attribute.                                                      |
| `prepend(element)`             | Adds an element to the start of a list attribute.                                                            |
| `prependList(elements)`        | Adds a list of elements to the start of a list attribute.                                                    |
| `add(number)`                  | Adds a number to a numeric attribute; if the attribute does not exist, it is set to the number.             |
| `addSet(set)`                  | Adds the elements of a set to a set attribute; if the attribute does not exist, it is set to the set.        |
| `deleteFromSet(set)`           | Removes the elements of a set from a set attribute.                                                          |
| `remove`                       | Removes the attribute from the item.                                                                         |
| `remove(index)`                | Removes the element at a zero-based index from a list attribute.                                            |

Every action is checked against the attribute's type, so `Person.age.set("old")` or `Person.name.add(1)` does not compile:

```scala mdoc:compile-only
val actions: UpdateExpression.Action[Person] =
  Person.nickNames.append("Johnny") +
    Person.nickNames.prependList(List("J")) +
    Person.tags.addSet(Set("vip")) +
    Person.tags.deleteFromSet(Set("new")) +
    Person.address.remove +
    Person.name.setIfNotExists("unknown")
```

## Options

These options apply to `update`:

```scala
<UPDATE_QUERY>
  .returns(<ReturnValues>)       // ReturnValues.None (default) | AllOld | UpdatedOld | AllNew | UpdatedNew
  .where(<ConditionExpression>)
  .metrics(<ReturnItemCollectionMetrics>)
  .capacity(<ReturnConsumedCapacity>)
```

By default the result is `None`. `returns(ReturnValues.AllNew)` returns the item after the update, decoded as the model, and a condition with `where` makes the update fail instead of creating or changing an item that does not match:

```scala mdoc:compile-only
val birthday: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery
    .update("person")(Person.id.partitionKey === "1")(Person.age.add(1))
    .where(Person.id.exists)
    .returns(ReturnValues.AllNew)
    .execute
```

`UpdatedOld` and `UpdatedNew` return only the updated attributes, which usually cannot be decoded as the whole model; the result is then `None`. Use them with [`updateItem`](../../low-level-api/index.md) when you need the partial item.
