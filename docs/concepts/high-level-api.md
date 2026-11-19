---
id: high-level-api
title: "High Level API"
---

The High Level API lets us read and write our own Scala types instead of raw DynamoDB items. It relies on two mechanisms:

1. A ZIO Schema derived for each case class or sealed trait, from which ZIO DynamoDB derives the codecs.
2. `ProjectionExpression`s generated from that schema, which name the model's fields in a type-safe way.

## ZIO Schema Derivation

The High Level API methods need an implicit `Schema` for the model type. We derive it with the `DeriveSchema.gen` macro and place it in the companion object, so it is always in implicit scope. ZIO DynamoDB uses the schema's metadata to encode the case class to a DynamoDB item and decode it back:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]

  val (id, name) = ProjectionExpression.accessors[Person]
}
```

## Projection Expression Generation

`ProjectionExpression.accessors` generates the projection expressions from the schema. For a case class it returns one `ProjectionExpression` per field, in field order; for a sealed trait it returns one per subtype. In the model above, `Person.id` has type `ProjectionExpression[Person, String]`: it points at the `id` attribute of a `Person` item and remembers that the attribute holds a `String`.

## Building Expressions From a ProjectionExpression

A `ProjectionExpression` is the starting point for every other expression: a condition, an update action and a primary key expression, each typed by the model it applies to:

```scala mdoc:silent
val condition: ConditionExpression[Person]         = Person.id === "1"
val action: UpdateExpression.Action[Person]        = Person.name.set("Smith")
val key: KeyConditionExpr.PrimaryKeyExpr[Person]   = Person.id.partitionKey === "1"
```

Comparing `Person.id` with a number, or combining a `Person` condition with a condition on another model, is a compile error.

## DynamoDBQuery CRUD Methods

The `DynamoDBQuery` companion object has type-safe query constructors such as `put`, `get`, `update`, `deleteFrom`, `scanAll` and `queryAll`, and they take the expressions above as arguments. Together they give us type-safe CRUD queries:

```scala mdoc:compile-only
import zio._

val table  = "person-table"
val person = Person("1", "John")

val program: ZIO[DynamoDBExecutor, Throwable, Unit] =
  for {
    _     <- DynamoDBQuery.put(table, person).where(!Person.id.exists).execute
    found <- DynamoDBQuery.get(table)(Person.id.partitionKey === "1").execute.absolve
    _     <- DynamoDBQuery.update(table)(Person.id.partitionKey === "1")(Person.name.set("Smith")).execute
    _     <- ZIO.debug(found == person) // true
  } yield ()
```

The [High Level API reference](../reference/high-level-api/index.md) documents every constructor, and [Creating Models](../reference/high-level-api/creating-models/index.md) shows how Scala types map to DynamoDB types.
