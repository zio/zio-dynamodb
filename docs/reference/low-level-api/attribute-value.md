---
id: attribute-value
title: "AttributeValue and AttrMap"
sidebar_label: "AttributeValue and AttrMap"
---

`AttributeValue` and `AttrMap` are the data types of the Low Level API: an `AttributeValue` is one DynamoDB value, and an `AttrMap` is a DynamoDB item, a map from attribute names to values.

## AttributeValue

The sealed trait `AttributeValue` corresponds one to one to an [attribute value in the AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html), with a case for each DynamoDB type (`S`, `N`, `B`, `BOOL`, `NULL`, `L`, `M`, `SS`, `NS`, `BS`). The cases themselves are internal; we create values from Scala values instead.

The type classes `ToAttributeValue` and `FromAttributeValue` convert Scala values to and from `AttributeValue`. They have instances for strings, numbers, booleans, binary data (`Iterable[Byte]`), `Option`, collections, maps with string keys and `AttrMap` itself. `AttributeValue.apply` uses `ToAttributeValue`:

```scala mdoc
import zio.dynamodb._

AttributeValue("hello")
AttributeValue(42)
AttributeValue(Set("a", "b"))
```

`AttributeValue.encode` and `AttributeValue#decode` convert a value of any type that has a ZIO Schema, using the same codecs as the High Level API:

```scala mdoc
import zio.schema.{ DeriveSchema, Schema }

case class Address(city: String, postcode: String)
object Address {
  implicit val schema: Schema.CaseClass2[String, String, Address] = DeriveSchema.gen[Address]
}

val encoded = AttributeValue.encode(Address("London", "SW1A 1AA"))
encoded.decode[Address]
```

You rarely build `AttributeValue`s yourself: `AttrMap` converts the values for us.

## AttrMap

An `AttrMap` represents a [DynamoDB item](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html), conceptually a `Map[String, AttributeValue]`. Rather than creating an `AttributeValue` per field, we pass plain Scala values and `ToAttributeValue` converts them. The type aliases `Item` and `PrimaryKey` name the two roles an `AttrMap` plays:

```scala mdoc:silent
val attrMap = AttrMap("id" -> "1", "age" -> 30)
val item    = Item("id" -> "1", "age" -> 30, "address" -> Item("city" -> "London", "postcode" -> "SW1A 1AA"))
val pk      = PrimaryKey("id" -> "1")
```

`AttrMap.empty` is the empty item, and `AttrMap#+` adds one attribute.

### Reading Fields

`AttrMap` has methods that read a field and convert it with `FromAttributeValue`:

```scala
final case class AttrMap(map: Map[String, AttributeValue]) {
  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[ItemError, A]
  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]]

  // fields that are items themselves
  def getItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, A]
  def getOptionalItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, Option[A]]
  def getIterableItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, Iterable[A]]
  def getOptionalIterableItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, Option[Iterable[A]]]
}
```

`AttrMap#get` returns a `DecodingError` when the field is missing or has the wrong type. `AttrMap#getOptional` returns `None` in both cases, so it cannot tell a missing field from a malformed one.

To decode a nested item with an optional field:

```scala mdoc
case class Foo(s: String, o: Option[String])

val nested = AttrMap("f1" -> AttrMap("f2" -> "a", "f3" -> "b"))

nested.getOptionalItem("f1") { m =>
  for {
    s <- m.get[String]("f2")
    o <- m.getOptional[String]("f3")
  } yield Foo(s, o)
}
```

### Decoding a Whole Item

`AttrMap#as` reads two to 22 fields and passes them to a function, which is a compact way to decode an item into a case class by hand:

```scala mdoc
case class Person(id: String, age: Int)

attrMap.as[String, Int, Person]("id", "age")(Person.apply)
```
