---
id: attribute-value
title: "AttributeValue"
---

# AttributeValue
The sealed trait `AttributeValue` has a one to one correspondence with the concept of an [attribute value in the AWS DDB API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html). It has implementations for all the types that are supported by DynamoDB

Internally there are the type classes `ToAttributeValue` and `FromAttribute` to convert between Scala types and `AttributeValue` and vice versa
whenever these conversions are required. Some example constructors are show below:

```scala
val s = AttributeValue.String("hello")
val bool = AttributeValue.Boolean(true)
```

However even when working with the Low Level API you would not use these constructors directly, instead you would use the `AttrMap` class (or more likely its type aliases) which is a convenience container for working with `AttributeValue` instances (see next section).

# AttrMap
An `AttrMap` is a convenience container for working an [DDB Item](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html) and **cuts down on boilerplate code when working with the Low Level API**. Conceptually it is a map of field name to AttributeValue `Map[String, AttributeValue]`.
However rather than requiring you to create an `AttributeValue` instance manually for each field, you can work with literal Scala types and type classes will handle the conversion to `AttributeValue` for you. There are also the the `PrimaryKey` and `Item` type aliases for `AttrMap` for readability as these structures follow the same pattern.

```scala
Some examples are shown below:

```scala
val attrMap = AttrMap("id" -> "1", "age" -> 30) 
val item = Item("id" -> "1", "age" -> 30) 
val pk = PrimaryKey("id" -> "1", "count" -> 30) 
// AttrMaps can also be nested 
val item = Item("id" -> "1", "age" -> 30, "address" -> Item("city" -> "London", "postcode" -> "SW1A 1AA")) 
```

To demonstrate the the reduction in boilerplate, if we were to create the first example manually it would look like this:

```scala
val attrMap = Map("id" -> AttributeValue.String("1"), "age" -> AttributeValue.Number(30))
```




