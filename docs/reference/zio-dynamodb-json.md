---
id: zio-dynamodb-json
title: "ZIO DynamoDB JSON Module"
sidebar_label: "ZIO DynamoDB JSON"
---

`zio-dynamodb-json` is an experimental, optional module for debugging and troubleshooting. It renders DynamoDB items in the JSON format that the AWS CLI and Console use, and parses that format back. It uses the same codecs as the rest of the library, so it shows exactly how a value is stored. It works at the level of `AttributeValue`, so it serves both the High Level and the Low Level API.

Typical uses are:

- Seeing how a case class is stored while designing a model.
- Checking a model's mapping in a unit test.
- Decoding an item copied from the AWS Console into a case class while investigating a production issue.

The module does not support the binary (`B`) and binary set (`BS`) types yet: parsing them returns a `DecodingError`, and rendering a value that contains them throws a `NotImplementedError`. It builds a ZIO JSON AST, which ZIO JSON itself avoids for speed, so it is not meant for production code paths.

## Installation

Add the module next to the core module in `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb-json" % "@VERSION@"
```

Its API comes with one import:

```scala mdoc:silent
import zio.dynamodb.json._
```

## API

The module adds these methods:

| Method                                                                           | Description                                                                         |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `a.toJsonString[A2 >: A]`, `a.toJsonStringPretty[A2 >: A]`                       | Encodes a value that has a `Schema[A2]` to compact or pretty-printed DynamoDB JSON. |
| `attrMap.toJsonString`, `attrMap.toJsonStringPretty`                             | Renders an `AttrMap` (an `Item`) as DynamoDB JSON.                                 |
| `parse[A: Schema](json: String): Either[DynamoDBError.ItemError, A]`             | Decodes DynamoDB JSON into an `A`.                                                  |
| `parseItem(json: String): Either[DynamoDBError.ItemError, AttrMap]`              | Decodes DynamoDB JSON into an `Item`.                                               |

The type parameter of `toJsonString` picks the schema used for encoding. For a subtype of a sealed trait, `value.toJsonString[Trait]` encodes it the way `put[Trait]` stores it, with its discriminator (see [put](high-level-api/crud-operations/put.md#storing-a-top-level-sum-type)).

## Visualizing How a Case Class Is Stored

During model development, `toJsonStringPretty` shows the DynamoDB representation of a value:

```scala mdoc
import java.time.Instant

import zio.schema.{ DeriveSchema, Schema }

case class Person(email: String, hobbies: Map[String, List[String]], friends: Set[String], registrationDate: Instant)
object Person {
  implicit val schema: Schema.CaseClass4[String, Map[String, List[String]], Set[String], Instant, Person] =
    DeriveSchema.gen[Person]
}

val person = Person("email", Map("sports" -> List("cricket", "football")), Set("John", "Tarlochan"), Instant.parse("2024-12-05T06:44:49Z"))

println(person.toJsonStringPretty[Person])
```

:::note
The rendered JSON writes nested maps, such as `hobbies` above, without the `"M"` type key that the AWS format uses (`"hobbies": { "M": { ... } }`). `parse` and `parseItem` accept both forms, but the AWS CLI does not accept the output for items with nested maps.
:::

[Creating Models](high-level-api/creating-models/index.md) describes the mapping of each Scala type, and [How to Customize How Sum Types Are Stored](../how-to/customize-sum-type-encoding.md) shows how annotations change it.

## Decoding JSON From the AWS Console

When investigating a production issue, we can copy an item's JSON from the AWS Console and decode it into a case class:

```scala mdoc
val json =
  """{
    "registrationDate": { "S": "2024-12-05T06:44:49Z" },
    "friends": { "SS": ["John", "Tarlochan"] },
    "hobbies": { "M": { "sports": { "L": [{ "S": "cricket" }, { "S": "football" }] } } },
    "email": { "S": "email" }
  }"""

parse[Person](json)
```

`parseItem` decodes the same JSON into an untyped `Item` instead:

```scala mdoc
parseItem("""{ "id": { "S": "1" }, "age": { "N": "42" } }""")
```

## Testing Mappings

Because `toJsonString` and `parse` are pure functions, they make simple unit tests for a model's mapping: assert that a value encodes to the JSON we expect, and that `parse` returns the original value. [How to Test Code That Uses DynamoDB](../how-to/test-dynamodb-code.md) shows where this fits among the other testing options.
