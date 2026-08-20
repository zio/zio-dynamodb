---
id: limitations
title: "Limitations"
---

## The High-Level API only models case classes and enums

The [High-Level API](crud/high-level.md) is built entirely on zio-blocks `Schema` derivation:
`derives Schema` on your model, `CompanionOptics`-generated `Lens`es for field access,
`DynamoDBCodecDeriver` for encode/decode. All three are built around zio-blocks' `Record`
(case classes) and `Variant` (`enum`/sealed traits) reflection shapes specifically — not an
open-ended "any Scala type" story.

In practice, that means:

- Your model must be a `case class` (optionally nesting other `case class`/`enum` fields), or
  an `enum`/sealed trait for a sum type — the two shapes `derives Schema` targets.
- Field access goes through `CompanionOptics`-generated `Lens`es (`Order.status`), which are
  themselves derived from the case class's constructor parameters — there's no equivalent for
  a getter/setter pair, a computed property, or a field guarded by custom validation logic in
  a method body.
- Mutation happens by producing new immutable values (or a `DynamoDBCodec`-driven
  `UpdateExpression.Action`), never by calling a method that mutates an object in place.

## Inheritance hierarchies: codecs support them, `CompanionOptics` doesn't

A sealed-trait hierarchy where a field is declared *abstractly* on a shared trait and
implemented by each concrete case — the classic OO shape — already round-trips fine through
`Schema`/`DynamoDBCodecDeriver`. The codec derivation doesn't care where a field is declared;
it reflects over each concrete case's actual constructor parameters:

```scala mdoc:compile-only
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

sealed trait Invoice { def id: Int }
sealed trait Billed extends Invoice { def amount: Double }

case class BilledMonthly(id: Int, amount: Double, month: Int) extends Billed
case class Prebilled(id: Int, count: Int)                     extends Invoice

object Invoice {
  implicit val schema: Schema[Invoice] = Schema.derived
}

val codec: DynamoDBCodec[Invoice] = Schema[Invoice].deriving(DynamoDBCodecDeriver).derive
val item                          = codec.toItem(BilledMonthly(1, 42.0, 3))
val back                          = codec.fromItem(item)
```

Due to a limitation with the `CompanionOptics` macro, abstract fields can't be used with
optics — so `.partitionKey`/`===`/`.set`/... aren't available for a field declared on the
abstract/intermediate trait rather than the concrete case.

Three workarounds solve the same underlying problem — a model with a field `CompanionOptics`
can't reach — ordered by how much work each takes:

1. **Use the Low-Level-API-plus-explicit-codec pattern** (least work) — for a model like the
   one above, where `Schema`/`DynamoDBCodecDeriver` already handle the shape and only
   `CompanionOptics` can't reach the field. Build the codec via
   `Schema[A].deriving(DynamoDBCodecDeriver).derive`, call `.toItem`/`.fromItem` directly, and
   write filter/condition expressions through the Low-Level API's dot-path syntax, naming the
   concrete case explicitly:
   ```scala
   $("BilledMonthly.amount") === 42.0
   ```
   See `OopModelWithAbstractFieldsSpec`
   (`schema-ddbexpr/src/test/scala/zio/dynamodb/OopModelWithAbstractFieldsSpec.scala`) for the
   full worked example, including matching across every case of a shared abstract field with
   an explicit `||`.
2. **Write a case class adapter** — for a model that can't derive a `Schema` at all, a plain
   `case class` DTO that mirrors just the fields you need to persist, `derives Schema`, and
   converts to/from your real domain type at the boundary. More work than option 1 (an extra
   type to maintain), but keeps the High-Level API's compile-time field/type checking for
   everything else.
3. **Use the [Low-Level API](crud/low-level.md) directly** — `Item`/`PrimaryKey` maps and
   `$("fieldName")` string-keyed access require no `Schema` at all. You write the
   encode/decode logic yourself (`ToAttributeValue`/`FromAttributeValue` instances, or manual
   `Item` construction). The most work, but no restriction whatsoever on what your in-memory
   model looks like.

## What already stays Low-Level only

Batch operations, table management, and (for now) transactions have no High-Level wrapper —
see [Batch Operations](crud/batch.md#why-no-high-level-batch-api) and
[High-Level: Transactions](crud/high-level.md#transactions) for the specific reasons behind
each; they aren't all the same reason, and none of them is *this* page's FP-modeling
restriction.
