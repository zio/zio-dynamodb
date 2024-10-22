---
id: put
title: "put"
---

```scala
def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]] = ???
```

The `put` operation is used to insert or replace an item in a table.

```scala
for {
  _ <- DynamoDBQuery.put("person", Person("1", "John", 21))
        .where(Person.id.notExists) // a ConditionExpression
        .execute
} yield ()
```

### `put` query combinators

```scala
<PUT_QUERY>
  .returns(<ReturnValues>) // ReturnValues.AllOld | ReturnValues.None <default>
  .where(<ConditionExpression>) // eg Person.id.notExists
```


