---
id: index
title: "Creating Models"
sidebar_label: "Creating Models"
---

The High Level API encodes our Scala types to DynamoDB items and decodes them back automatically. It needs an implicit ZIO `Schema` for each type, which we derive with `DeriveSchema.gen` and place in the companion object so that it is always in implicit scope:

```scala mdoc:silent
import java.time.Instant

import zio.dynamodb._
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }

case class Person(email: String, hobbies: Map[String, List[String]], registrationDate: Instant)
object Person {
  implicit val schema: Schema.CaseClass3[String, Map[String, List[String]], Instant, Person] =
    DeriveSchema.gen[Person]

  val (email, hobbies, registrationDate) = ProjectionExpression.accessors[Person]
}
```

ZIO DynamoDB derives a codec from the schema. Where possible it maps a Scala type to the matching native DynamoDB type, so a Scala `Map` becomes a DynamoDB map and a `Set[String]` a DynamoDB string set. The [ZIO DynamoDB JSON](../../zio-dynamodb-json.md) module shows the encoding in the JSON format of the AWS Console:

```scala mdoc
val person = Person("email", Map("sports" -> List("cricket", "football")), Instant.parse("2024-12-05T06:44:49Z"))

println(person.toJsonStringPretty[Person])
```

## Scalar Types

Scalar Scala types map to these DynamoDB types:

| Scala type                                             | DynamoDB type | Notes                                                                            |
| ------------------------------------------------------ | ------------- | -------------------------------------------------------------------------------- |
| `Unit`                                                 | `NULL`        |                                                                                  |
| `String`                                               | `S`           |                                                                                  |
| Numeric types (`Int`, `Long`, `BigDecimal`, ...)       | `N`           |                                                                                  |
| `Boolean`                                              | `BOOL`        |                                                                                  |
| `Chunk[Byte]`, `Byte`                                  | `B`           | Binary. Other collections of `Byte`, such as `List[Byte]`, are stored as a list (`L`) of binary values. |
| `java.time.*`                                          | `S`           | DynamoDB has no date type; values are stored as ISO-8601 strings.               |
| `java.util.UUID`                                       | `S`           | DynamoDB has no UUID type.                                                       |
| `java.util.Currency`                                   | `S`           | DynamoDB has no currency type.                                                   |

## Collection Types

In the table below, `A`, `K` and `V` can be scalar types, collections or case classes:

| Scala type               | DynamoDB type | Notes                                                                                     |
| ------------------------ | ------------- | ----------------------------------------------------------------------------------------- |
| `Option[A]`              |               | `Some` is stored as the value; `None` as the absence of the attribute.                   |
| `List[A]`, `Chunk[A]`    | `L`           |                                                                                           |
| `Set[String]`            | `SS`          |                                                                                           |
| `Set` of a numeric type  | `NS`          |                                                                                           |
| `Set` of binary          | `BS`          |                                                                                           |
| `Set[A]` of another type | `L`           | DynamoDB sets only hold strings, numbers and binary values.                               |
| `Map[String, A]`         | `M`           |                                                                                           |
| `Map[K, V]`              | `L`           | Other key types are stored as a list of key-value pairs.                                  |

DynamoDB sets cannot be empty, so an empty `Set` is stored as the absence of the attribute. When decoding, a missing attribute whose field is an `Option`, a `List`, a `Chunk`, a `Map` or a `Set` becomes `None` or the empty collection; a missing attribute whose field has any other type is a `DecodingError`. This lets us add collection fields to a model without migrating existing items:

```scala mdoc
case class Profile(email: String, tags: Set[String], scores: List[Int], links: Map[String, String])
object Profile {
  implicit val schema: Schema.CaseClass4[String, Set[String], List[Int], Map[String, String], Profile] =
    DeriveSchema.gen[Profile]
}

parse[Profile]("""{ "email": { "S": "a@b.com" } }""")
```

## Either

By default a Scala `Either` is stored as a map with a single key, `Left` or `Right`, holding the encoded value:

```scala mdoc
case class Result(value: Either[String, Int])
object Result {
  implicit val schema: Schema.CaseClass1[Either[String, Int], Result] = DeriveSchema.gen[Result]
}

println(Result(Left("error")).toJsonString[Result])
```

For a flatter, "union"-style encoding that stores just the value, put a schema for `Either` based on `Schema.Fallback` in implicit scope, for example in the model's companion object. Decoding tries the left schema first, then the right one:

```scala mdoc
case class FlatResult(value: Either[String, Int])
object FlatResult {
  implicit def fallbackEither[A, B](implicit schemaA: Schema[A], schemaB: Schema[B]): Schema[Either[A, B]] =
    Schema.Fallback(schemaA, schemaB).transform(_.toEither, zio.schema.Fallback.fromEither)

  implicit val schema: Schema.CaseClass1[Either[String, Int], FlatResult] = DeriveSchema.gen[FlatResult]
}

println(FlatResult(Left("error")).toJsonString[FlatResult])
println(FlatResult(Right(42)).toJsonString[FlatResult])
```

The [TypeSafeApiCrudSpec](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiCrudSpec.scala) integration test has a complete example.

## Sealed Traits and Annotations

Sealed traits are stored with a discriminator that identifies the subtype. The default encoding, and the ZIO Schema annotations (`@discriminatorName`, `@caseName`, `@fieldName`, `@noDiscriminator`, `@simpleEnum`) that change it, are covered in [How to Customize How Sum Types Are Stored](../../../how-to/customize-sum-type-encoding.md). ZIO Schema's limits on models, and how to work around them, are in [How to Model Data Within ZIO Schema's Limits](../../../how-to/model-data-within-schema-limits.md).

[Field Traversal](field-traversal.md) shows how to address nested fields of a model in expressions.
