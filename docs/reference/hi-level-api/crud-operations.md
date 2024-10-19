---
id: crud-operations
title: "CRUD Operations"
---

The High Level API mirrors the CRUD operations of DDB but with a high level of type safety. 

We have to start of with a Scala model of the table and add an implicit schema reference in the companion object together with some convenience `ProjectionExpression`'s via the `accessors` function. 

```scala
final case class Person(id: String, name: String, age: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]
  val (id, name, age) = ProjectionExpression.accessors[Person]
}
```

## `put`

```scala
def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]] = ???
```

The `put` operation is used to insert or replace an item in a table and can be combined 

```scala
for {
  _ <- DynamoDBQuery.put("Person", Person("1", "John", 21))
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


## `get`

```scala
  def get[From: Schema](tableName: String)(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]
  ): DynamoDBQuery[From, Either[ItemError, From]] = ???
```

The `get` operation is used to retrieve an item from a table. The `KeyConditionExpr.PrimaryKeyExpr` can be created using the `ProjectionExpression`'s in the companion object for model class. It returns an `Either[ItemError, From]` where `ItemError` is a sealed trait that that has `ValueNotFound` and `DecodingError` instances. 

```scala
for {
  errorOrPerson <- DynamoDBQuery.get("Person")(Person.id.primaryKey("1")).execute
} yield errorOrPerson
```

### Working with `get` return values

Sometimes working with a `Either[ItemError, From]` can be a little unwieldy so there are two approaches we can take.

The first approach is to use the ZIO `absolve` method to push all ItemErrors into the ZIO error channel

```scala
import zio.dynamodb.syntax._
for {
  person <- DynamoDBQuery.get("Person")(Person.id.primaryKey("1")).execute.absolve
} yield person
```

However sometimes we wish to treat `NotFound` as a success case and for this the `maybeFound` extension method can be imported to push the `DecodingError` into the ZIO error channel and handle `NotFound` as a successful operation by using an `Option` type. 

```scala
import zio.dynamodb.syntax._
for {
  maybePerson <- DynamoDBQuery.get("Person")(Person.id.primaryKey("1")).execute.maybeFound
} yield maybePerson
```

### `get` query combinators

```scala
<GET_QUERY>.where(<ConditionExpression>)
```

## `update`

```scala
def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(
    action: Action[From]
): DynamoDBQuery[From, Option[From]]  = ???
```

The update operation is used to modify an existing item in a table. Both `KeyConditionExpr.PrimaryKeyExpr` and the `Action` params can be created using the `ProjectionExpression`'s in the companion object for model class, eg: 
    
```scala
for {
  _ <- DynamoDBQuery.update("person")(Person.id.primaryKey === "1")(
    Person.name.set(42) + Person.age.set(42)
  ).execute
} yield maybePerson
```

### `put` query combinators

```scala
<PUT_QUERY>
  .returns(<ReturnValues>) // ReturnValues.AllNew | ReturnValues.AllOld | ReturnValues.None <default>
  .where(<ConditionExpression>)
```
