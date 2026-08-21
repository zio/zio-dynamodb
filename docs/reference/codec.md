---
id: codec
title: "Codec Configuration"
---

`DynamoDBCodecDeriver` turns a zio-blocks `Schema[A]` into a `DynamoDBCodec[A]`
(encode/decode between `A` and `AttributeValue`). It's what both API levels use under the
hood — the [High-Level API](crud/high-level.md) invokes it automatically for every
`derives Schema` model; the [Low-Level API](crud/low-level.md) can invoke it directly when
you want a codec without going through `put`/`get`/... at all:

```scala mdoc:compile-only
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

case class Order(customerId: String, orderId: String, total: Double) derives Schema

val codec: DynamoDBCodec[Order] = Schema[Order].deriving(DynamoDBCodecDeriver).derive
val item                        = codec.toItem(Order("cust-42", "ord-1", 129.99))
val back                        = codec.fromItem(item)
```

Configuration works at two levels: deriver-wide factory parameters (`withXxx` methods,
applied to every type the deriver touches) and per-type/per-field `@Modifier` annotations
(override the deriver-wide setting for just that type or field).

## Deriver-wide factory parameters

`DynamoDBCodecDeriver` is immutable — every `withXxx` method returns a new configured
instance rather than mutating the shared default:

```scala mdoc:compile-only
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver
import zio.blocks.schema.json.DiscriminatorKind

case class Order(customerId: String, orderId: String) derives Schema

val configured = DynamoDBCodecDeriver
  .withEnumValuesAsStrings(false)
  .withDiscriminatorKind(DiscriminatorKind.Field("_type"))

val codec = Schema[Order].deriving(configured).derive
```

| Method | Default | Effect |
|---|---|---|
| `withEnumValuesAsStrings` | `true` | Field-less sealed-trait cases encode as bare `AttributeValue.String("CaseName")` when `true`; `{"CaseName": {}}` (Map-wrapped) when `false`. |
| `withDiscriminatorKind` | `DiscriminatorKind.Key` | `Key` wraps a case's fields under its name (`{"CaseName": {...}}`); `Field(name)` flattens a discriminator field alongside the real fields (`{...fields, name: "CaseName"}`); `None` writes no case marker at all. |
| `withFieldNameMapper` | `NameMapper.Identity` | Renaming strategy applied to every field name, unless overridden per-type by `@Modifier.fieldNaming`. |
| `withCaseNameMapper` | `NameMapper.Identity` | Same, for sealed-trait case names; overridden per-type by `@Modifier.caseNaming`. |
| `withTransientNone` | `true` | `None` fields are omitted from the encoded map when `true`; written as `AttributeValue.Null` when `false`. |
| `withRequiredCollectionFields` | `false` | Whether a missing collection key on decode is an error (`true`) or treated as empty (`false`). |
| `withTransientEmptyCollection` | `false` | Whether an empty collection is omitted from the encoded map (`true`) rather than written as an empty `List`/native empty set. |
| `withTransientDefaultValue` | `false` | Whether a field whose value equals its declared default is omitted from the encoded map. |
| `withRequireDefaultValueFields` | `false` | Whether a missing key for a field with a declared default is an error, rather than falling back to the default. |
| `withRejectExtraFields` | `false` | Whether an unrecognized key in the decoded map is an error (`true`), overridden per-type by `@Modifier.noExtraFields`. |
| `withSchema1TupleCompatibility` / `withSchema1ByteSequenceCompatibility` / `withSchema1ByteCompatibility` / `withSchema1YearCompatibility` | see [Schema1Compat](#schema1compat-migrating-from-2x) below | 2.x wire-format migration knobs. |

`NameMapper.fromString` accepts `"identity"`, `"snake_case"`, `"camelCase"`, `"kebab-case"`,
`"PascalCase"`.

## Per-type and per-field `@Modifier` annotations

Annotations override the deriver-wide setting for just the annotated type or field —
useful when most of your models should follow one convention but a specific type needs to
match an existing table's wire format:

```scala mdoc:compile-only
import zio.blocks.schema.{ Modifier, Schema }

@Modifier.discriminator("_type")   // this type only: Field-style, not the deriver's default
@Modifier.caseNaming("snake_case") // this type's case names only: snake_case
sealed trait Shape derives Schema
object Shape {
  case class Circle(radius: Int)                     extends Shape
  case class Rect(@Modifier.rename("w") width: Int, @Modifier.rename("h") height: Int) extends Shape
}
```

| Annotation | Applies to | Effect |
|---|---|---|
| `@Modifier.discriminator(name)` | sealed trait/`enum` | Overrides `withDiscriminatorKind` to `Field(name)` for this type only. |
| `@Modifier.caseNaming(strategy)` | sealed trait/`enum` | Overrides `withCaseNameMapper` for this type's case names only. |
| `@Modifier.fieldNaming(strategy)` | case class | Overrides `withFieldNameMapper` for this type's field names only. |
| `@Modifier.noExtraFields()` | case class | Overrides `withRejectExtraFields` to `true` for this type only. |
| `@Modifier.rename(name)` | field or case | Literal rename on encode/decode — the direct per-item equivalent of a naming-strategy mapper. |
| `@Modifier.alias(name)` | field or case | An additional name accepted on decode (alongside the primary name), without changing what's written on encode. |
| `@Modifier.transient()` | field | Field is excluded from both encode and decode; always takes its default value on decode. |
| `@Modifier.encodeTransient()` | field | Field is excluded from encode only; still read (or defaulted) on decode. |

## Configuring the deriver the High-Level API uses

`put`/`get`/`update`/... don't take a `DynamoDBCodecDeriver` argument directly — they derive
one implicitly via `DynamoDBCodecDeriverConfigure[A]`, which defaults to the unconfigured
deriver. Supply your own instance in scope to change the configuration for a specific model
type used through the High-Level API:

```scala mdoc:compile-only
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure

case class Order(customerId: String, orderId: String) derives Schema

implicit val orderCodecConfig: DynamoDBCodecDeriverConfigure[Order] =
  _.withEnumValuesAsStrings(false)
```

## `Schema1Compat`: migrating from 2.x

Four types encode differently by default in 3.x than they did in 2.x's `zio-schema`-based
codec:

| Type | 2.x wire format | 3.x default wire format | Knob |
|---|---|---|---|
| Byte-sequence collections (`Chunk[Byte]`, `Array[Byte]`) | `List` of per-byte `Number` | `Binary` (zero-copy for `Array[Byte]`) | `withSchema1ByteSequenceCompatibility` |
| Tuples (arity ≥ 2) | Right-folded nested pairs — `List(List(List(a,b),c),d)` | Flat positional list — `List(a,b,c,d)` | `withSchema1TupleCompatibility` |
| `Year` | Zero-padded 4-digit `String` | `Number` | `withSchema1YearCompatibility` |
| Standalone `Byte` (not in a collection) | `Binary` of length 1 | `Number` | `withSchema1ByteCompatibility` |

Each row's knob takes a `Schema1Compat` value governing how to bridge the two formats during
a rolling migration:

```scala
sealed abstract class Schema1Compat
object Schema1Compat {
  case object ReadNewWriteNew  extends Schema1Compat // write + read only the new format
  case object ReadBothWriteOld extends Schema1Compat // write old format; decode both
  case object ReadBothWriteNew extends Schema1Compat // write new format; decode both
}
```

The intended migration path for a live table, using any of the four knobs:

1. **`ReadBothWriteOld`** — deploy the new library while still writing the old format; safe
   to run alongside instances still on the old library.
2. **`ReadBothWriteNew`** — flip to writing the new format; old records already in the table
   still decode correctly via the fallback path.
3. **`ReadNewWriteNew`** (the default) — once all data has been rewritten or aged out; no
   fallback overhead at decode time.

Two more defaults changed without a compat knob, since both are additive rather than
migratory — old and new data don't coexist in the same table field:

- **`enumValuesAsStrings` defaults to `true`.** A 2.x sealed trait of case objects with no
  annotations produced `{"CaseName": {}}`; the 3.x default produces bare `"CaseName"`
  instead. `withEnumValuesAsStrings(false)` restores the 2.x shape.
- **`DiscriminatorKind.None`'s ambiguity handling differs from 2.x's `@noDiscriminator`.**
  2.x tries every candidate case's decoder and raises a hard error if more than one
  succeeds; 3.x tries each case in declaration order and returns the first success, with no
  ambiguity check. For models whose cases have field-name or field-type overlap (not just
  different field *counts*), this can silently resolve to the wrong case where 2.x would
  have refused to guess. Structurally distinct cases (different field names, or the same
  name at a different type) aren't affected — the "wrong" candidate's decoder fails on its
  own regardless of try order.
