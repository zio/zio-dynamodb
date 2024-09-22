---
id: high-level-api
title: "High Level API"
---

# High Level API
The High Level API relies on automatically derived ZIO Schema's using the `DeriveSchema.gen` macro being in implicit scope. Internally codecs are automatically generated for the case classes based on the meta data provided by the Schema's.

The High Level API is made up of: 
- type safe query creation methods in the DynamoDBQuery such are `get`, `put` etc etc
- automatically defined `ProjectionExpression`s that act a springboard for creating further type safe APIs ie:
  - **`ConditionExpression`** eg `Person.id === "1"`
  - **`UpdateExpression`** eg `Person.name.set("Smith") + Person.age.set(30)`
  - **primary key** expressions eg `Person.id.partitionKey === "1"`

