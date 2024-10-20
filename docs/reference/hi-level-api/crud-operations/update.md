---
id: update
title: "update"
---


```scala
def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(
    action: Action[From]
): DynamoDBQuery[From, Option[From]]  = ???
```

The update operation is used to modify an existing item in a table. Both `KeyConditionExpr.PrimaryKeyExpr` and the `Action` params can be created using the `ProjectionExpression`'s in the companion object for model class: 
    
```scala
for {
  _ <- DynamoDBQuery.update("person")(Person.id.primaryKey === "1")(
    Person.name.set(42) + Person.age.set(42)
  ).execute
} yield maybePerson
```

### `put` query combinators

```scala
<UPDATE_QUERY>
  .returns(<ReturnValues>) // ReturnValues.AllNew | ReturnValues.AllOld | ReturnValues.None <default>
  .where(<ConditionExpression>)
```
