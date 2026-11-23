---
id: get
title: "get"
---

`DynamoDBQuery.get` reads the item with the given primary key and decodes it:

```scala
object DynamoDBQuery {
  def get[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[From, Either[ItemError, From]]
}
```

We build the primary key expression from the model's projection expressions (see [Primary Keys](../primary-keys.md)). The examples use this model:

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

`DynamoDBQuery.get` returns an `Either[ItemError, From]`: a `Left(ItemError.ValueNotFound)` when no item has the key, a `Left(ItemError.DecodingError)` when the item does not match the schema, and a `Right` with the value otherwise:

```scala mdoc:compile-only
val errorOrPerson: ZIO[DynamoDBExecutor, DynamoDBError, Either[DynamoDBError.ItemError, Person]] =
  DynamoDBQuery.get("person")(Person.id.partitionKey === "1").execute
```

`DynamoDBQuery.get` only reads the attributes that the schema's fields name, so attributes the model does not know about are not transferred.

## Working With get Return Values

An `Either` inside the result is often unwieldy, so there are two ways to flatten it. `ZIO#absolve` moves both kinds of `ItemError` into the ZIO error channel:

```scala mdoc:compile-only
val person: ZIO[DynamoDBExecutor, DynamoDBError, Person] =
  DynamoDBQuery.get("person")(Person.id.partitionKey === "1").execute.absolve
```

When a missing item is a normal outcome, the `maybeFound` extension method from `zio.dynamodb.syntax._` returns `None` for `ValueNotFound` and moves only `DecodingError` into the error channel:

```scala mdoc:compile-only
import zio.dynamodb.syntax._

val maybePerson: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery.get("person")(Person.id.partitionKey === "1").execute.maybeFound
```

## Options

These options apply to `DynamoDBQuery.get`:

```scala
<GET_QUERY>
  .consistency(<ConsistencyMode>)   // ConsistencyMode.Strong | ConsistencyMode.Weak (default)
  .capacity(<ReturnConsumedCapacity>)
```

A strongly consistent read reflects every write that succeeded before it:

```scala mdoc:compile-only
val latest: ZIO[DynamoDBExecutor, DynamoDBError, Either[DynamoDBError.ItemError, Person]] =
  DynamoDBQuery.get("person")(Person.id.partitionKey === "1").consistency(ConsistencyMode.Strong).execute
```

## Reading a Subtype of a Top-Level Sum Type

When a table stores the subtypes of a sealed trait (see [Storing a Top-Level Sum Type](put.md#storing-a-top-level-sum-type)), the experimental `DynamoDBQuery.getWithNarrow[From, To]` reads an item as the sealed trait `From` and narrows it to the subtype `To`. If the stored item is another subtype, the result is a `Left(DecodingError)`:

```scala
object DynamoDBQuery {
  def getWithNarrow[From: Schema.Enum, To <: From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[To]): DynamoDBQuery[From, Either[ItemError, To]]
}
```

For example, with the `Invoice` model from the put page, `getWithNarrow[Invoice, Invoice.Unpaid]("invoice")(Invoice.Unpaid.id.partitionKey === "1")` returns a `Right(Unpaid("1"))` for an unpaid invoice and a `Left` for a paid one.
