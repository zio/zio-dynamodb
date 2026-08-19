---
id: limitations
title: "Limitations"
---

## The High-Level API only models algebraic data types

The [High-Level API](crud/high-level.md) is built entirely on zio-blocks `Schema` derivation:
`derives Schema` on your model, `CompanionOptics`-generated `Lens`es for field access,
`DynamoDBCodecDeriver` for encode/decode. All three are built around zio-blocks' `Record`
(product types — case classes) and `Variant` (sum types — `enum`/sealed traits) reflection
shapes specifically — the two canonical FP algebraic-data-type shapes, not an open-ended "any
Scala type" story.

In practice, that means:

- Your model must be a `case class` (optionally nesting other `case class`/`enum` fields), or
  an `enum`/sealed trait for a sum type — the two shapes `derives Schema` targets.
- Field access goes through `CompanionOptics`-generated `Lens`es (`Order.status`), which are
  themselves derived from the case class's constructor parameters — there's no equivalent for
  a getter/setter pair, a computed property, or a field guarded by custom validation logic in
  a method body.
- Mutation happens by producing new immutable values (or a `DynamoDBCodec`-driven
  `UpdateExpression.Action`), never by calling a method that mutates an object in place.

## Working with OOP-style models

If your domain model is a mutable class, a JavaBean-style type with getters/setters, an ORM
entity, or anything else that doesn't reduce to a case class/enum shape, you have two options:

1. **Write a case class adapter** — a plain `case class` DTO that mirrors just the fields you
   need to persist, `derives Schema`, and convert to/from your real domain type at the
   boundary. This is usually the least-friction option and keeps the HL API's compile-time
   field/type checking.
2. **Use the [Low-Level API](crud/low-level.md) directly** — `Item`/`PrimaryKey` maps and
   `$("fieldName")` string-keyed access require no `Schema` at all. You write the
   encode/decode logic yourself (`ToAttributeValue`/`FromAttributeValue` instances, or manual
   `Item` construction), in exchange for no restriction on what your in-memory model looks
   like.

## What already stays Low-Level only

Batch operations, table management, and (for now) transactions have no High-Level wrapper —
see [Batch Operations](crud/batch.md#why-no-high-level-batch-api) and
[High-Level: Transactions](crud/high-level.md#transactions) for the specific reasons behind
each; they aren't all the same reason, and none of them is *this* page's FP-modeling
restriction.
