---
id: customize-sum-type-encoding
title: "How to Customize How Sum Types Are Stored"
sidebar_label: "Customize Sum Type Encoding"
description: "Control how sealed traits and their cases are stored in DynamoDB with the ZIO Schema annotations discriminatorName, caseName, fieldName and noDiscriminator."
keywords:
  - "Codec Customization"
  - "Sum Types"
  - "Sealed Trait Encoding"
  - "discriminatorName"
  - "noDiscriminator"
---

In this guide, we choose how ZIO DynamoDB stores a sealed trait and its cases. By the end, you will know the default encoding, how to switch to a flat encoding with a discriminator field, how to rename cases and fields, and how to read data written without any discriminator.

We look at the default encoding first, then change it step by step with annotations, and finish with recommendations for new tables.

## The Problem

The High Level API derives a codec for every model from its ZIO Schema, and for a sealed trait it needs some way to record which case an item holds. The default choice nests the case inside an extra map keyed by the case's class name. That works, but the data may need a different shape: an existing table may already store a type field next to the other attributes, a name may need to differ from the Scala class name, or a top-level sealed trait needs its partition key at the top of the item, where the default nesting cannot put it.

Writing such codecs by hand would mean converting every case to and from `AttributeValue` ourselves. Instead, we annotate the model and let the derived codec follow the annotations.

## Prerequisites

The annotations are ZIO Schema's, from `zio.schema.annotation._`; ZIO DynamoDB's codecs read them. To see the stored form, we use the optional `zio-dynamodb-json` module, whose `toJsonString` renders a value with its DynamoDB types (see [ZIO DynamoDB JSON Module](../reference/zio-dynamodb-json.md)). Add it next to the core module:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb"      % "@VERSION@",
  "dev.zio" %% "zio-dynamodb-json" % "@VERSION@"
)
```

Every example on this page defines a `TrafficLight` sealed trait and stores it in the `colour` field of a `Box`.

## The Default Encoding

Without annotations, a case class case is stored as a map with a single entry: the key is the case's name and the value is the encoded case class. A `Box(Green(42))` is stored like this:

```scala mdoc:silent:reset
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }

sealed trait TrafficLight
case class Green(rgb: Int) extends TrafficLight
case class Red(rgb: Int)   extends TrafficLight

case class Box(colour: TrafficLight)
object Box {
  implicit val schema: Schema[Box] = DeriveSchema.gen[Box]
}
```

Rendering a `Box(Green(42))` shows the stored form:

```scala mdoc
println(Box(Green(42)).toJsonString)
```

The extra `"Green"` map identifies the case. When every case of the sealed trait is a case object, ZIO Schema's derivation marks the trait as a simple enum, and each case is stored as a plain string instead:

```scala mdoc:silent:reset
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }

sealed trait TrafficLight
case object GREEN extends TrafficLight
case object RED   extends TrafficLight

case class Box(colour: TrafficLight)
object Box {
  implicit val schema: Schema[Box] = DeriveSchema.gen[Box]
}
```

A `Box(GREEN)` is then stored with a string field:

```scala mdoc
println(Box(GREEN).toJsonString)
```

## Renaming a Case With caseName

The `@caseName` annotation on a case replaces its class name in the stored data. It works with every encoding on this page; the one exception is a case class under `@noDiscriminator` (described below), which is stored as its fields only:

```scala mdoc:silent:reset
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.annotation._

sealed trait TrafficLight
case object GREEN extends TrafficLight
@caseName("red_traffic_light")
case object RED   extends TrafficLight

case class Box(colour: TrafficLight)
object Box {
  implicit val schema: Schema[Box] = DeriveSchema.gen[Box]
}
```

The renamed case is stored under its new name:

```scala mdoc
println(Box(RED).toJsonString)
```

## Storing a Discriminator Field With discriminatorName

The `@discriminatorName` annotation on the sealed trait removes the extra map. The case's fields are stored directly, plus one more field, named by the annotation, whose value is the case's name. `@caseName` changes that value, and `@fieldName` on a case class parameter changes a field's stored name:

```scala mdoc:silent:reset
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.annotation._

@discriminatorName("light_type")
sealed trait TrafficLight
case class Green(rgb: Int) extends TrafficLight
@caseName("red_traffic_light")
case class Red(rgb: Int) extends TrafficLight
case class Amber(@fieldName("red_green_blue") rgb: Int) extends TrafficLight

case class Box(colour: TrafficLight)
object Box {
  implicit val schema: Schema[Box] = DeriveSchema.gen[Box]
}
```

The three cases are stored as:

```scala mdoc
println(Box(Green(42)).toJsonString)
println(Box(Red(42)).toJsonString)
println(Box(Amber(42)).toJsonString)
```

This flat form is also the only one that works for a sealed trait stored as a whole item, because the partition key must be a top-level attribute. When we write such an item, we pass the trait as the type parameter, `DynamoDBQuery.put[Invoice](...)`, so the discriminator is written; see [put](../reference/high-level-api/crud-operations/put.md).

## Reading Data Without a Discriminator

Some mapping libraries store sum types with no discriminator at all. To read such data, we annotate the trait with `@noDiscriminator`. A case object is then stored as its name, and a case class as its fields only, so `@caseName` on a case class has no effect:

```scala mdoc:silent:reset
import zio.dynamodb.json._
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.annotation._

@noDiscriminator
sealed trait TrafficLight
@caseName("blue")
case object Blue extends TrafficLight
case object Purple extends TrafficLight
case class Green(rgb: Int, i: Int) extends TrafficLight
case class Amber(@fieldName("red_green_blue") rgb: Int) extends TrafficLight

case class Box(colour: TrafficLight)
object Box {
  implicit val schema: Schema[Box] = DeriveSchema.gen[Box]
}
```

The case object is stored as its name, and the case class as its fields alone:

```scala mdoc
println(Box(Blue).toJsonString)
println(Box(Amber(42)).toJsonString)
```

To decode a map, the codec tries every case class whose number of fields matches the number of attributes in the map, and exactly one must succeed. Here only `Amber` has one field, so the stored `Amber` decodes back:

```scala mdoc
parse[Box]("""{"colour":{"red_green_blue":{"N":"42"}}}""")
```

If no case succeeds, or more than one does, decoding fails with a `DynamoDBError.ItemError.DecodingError`. Two cases with the same number of fields and compatible field types are therefore ambiguous.

:::warning
`@noDiscriminator` is slower, because decoding tries several cases, and it relies on every case looking different once stored. Use it only to read data you cannot change.
:::

## Choosing an Encoding for New Tables

For a new table:

- Use the default encoding for sum types nested inside a case class.
- Use `@discriminatorName` when you prefer the flat form, and always for a sealed trait stored as a whole item.
- For deep hierarchies, avoid a top-level sealed trait altogether: store a concrete case class whose fields are product and sum types, as [How to Model Data Within ZIO Schema's Limits](model-data-within-schema-limits.md) shows.

## Going Further

- [Creating Models](../reference/high-level-api/creating-models/index.md) lists how every Scala type maps to a DynamoDB type.
- [Field Traversal](../reference/high-level-api/creating-models/field-traversal.md) shows how to reach the fields of a sum type case in expressions.
- [ZIO DynamoDB JSON Module](../reference/zio-dynamodb-json.md) covers `toJsonString` and `parse`, which make the encodings easy to check in unit tests.
