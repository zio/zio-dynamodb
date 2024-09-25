---
id: attribute-value
title: "AttributeValue"
---

# AttributeValue
The sealed trait `AttributeValue` has a one to one correspondence with the concept of an [attribute value in the AWS DDB API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html). It has implementations for all the types that are supported by DynamoDB

Internally there are type classes to convert between Scala types and `AttributeValue` and vice versa.

# AttrMap
An `AttrMap` is a convenience container for working an [DDB Item](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html) and conceptually is a map of field name to AttributeValue `Map[String, AttributeValue]`.
However rather than requiring you to create an `AttributeValue` instance manually for each field, you can work with literal Scala types and the `AttrMap` will handle the conversion to `AttributeValue` for you. 


