---
id: low-level-api
title: "Low Level API"
---

# Low Level API
The low level API provides low level query creation and execution and is based on abstractions at the DynamoDB level such as:
- `AttributeValue` 
- `AttrMap` which is a convenience container with automatic conversion between Scala values and `AttributeValue`'s. It also has type aliases of `PrimaryKey` and `Item`
- `$` function for creating untyped `ProjectionExpression`s eg `$("person.address.houseNumber")`
