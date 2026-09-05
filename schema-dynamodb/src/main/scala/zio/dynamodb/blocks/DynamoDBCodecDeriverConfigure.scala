/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb.blocks

import zio.blocks.schema.{ Modifier, NameMapper }
import zio.blocks.schema.derive.Deriver
import zio.blocks.schema.json.DiscriminatorKind
import zio.blocks.typeid.TypeId
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver, Resolver, ResolverDeriver, Schema1Compat }

/**
 * Codec-derivation policy for a type `A`, held as a value with readable fields rather than
 * an opaque `DynamoDBCodecDeriver => Deriver` lambda — so the library can inspect
 * individual settings (e.g. `fieldNameMapper` / `discriminatorKind` / the per-field
 * `Modifier.rename`s when resolving an optic to a DynamoDB attribute path), not just apply
 * them.
 *
 * The scalar fields mirror the `DynamoDBCodecDeriver` constructor knobs — derivation
 * policy, usually codebase-wide. `withModifier` / `withInstance` keep the exact names and
 * signatures they have on `Deriver`; the calls are recorded (not applied opaquely) and
 * replayed by [[toDeriver]]. `@Modifier` annotations on the datatype do the same thing and
 * both are honoured (the deriver merges them).
 *
 * `A` is phantom — present only so `given DynamoDBCodecDeriverConfigure[Foo]` resolves per
 * type.
 */
final case class DynamoDBCodecDeriverConfigure[A](
  fieldNameMapper: NameMapper = NameMapper.Identity,
  caseNameMapper: NameMapper = NameMapper.Identity,
  discriminatorKind: DiscriminatorKind = DiscriminatorKind.Key,
  enumValuesAsStrings: Boolean = true,
  rejectExtraFields: Boolean = false,
  transientNone: Boolean = true,
  requireOptionFields: Boolean = false,
  transientEmptyCollection: Boolean = false,
  requireCollectionFields: Boolean = false,
  transientDefaultValue: Boolean = false,
  requireDefaultValueFields: Boolean = false,
  schema1TupleCompat: Schema1Compat = Schema1Compat.ReadNewWriteNew,
  schema1ByteSequenceCompat: Schema1Compat = Schema1Compat.ReadNewWriteNew,
  schema1ByteCompat: Schema1Compat = Schema1Compat.ReadNewWriteNew,
  schema1YearCompat: Schema1Compat = Schema1Compat.ReadNewWriteNew,
  termModifiers: Vector[(TypeId[Any], String, Modifier.Term)] = Vector.empty,
  typeModifiers: Vector[(TypeId[Any], Modifier.Reflect)] = Vector.empty,
  instanceOverrides: Vector[(TypeId[Any], DynamoDBCodec[Any])] = Vector.empty
) {

  // Cached — this value keys DerivedCodecSyntax's per-`===`/`>` codec cache; the default
  // instance is a shared singleton, so its hash is computed once for the whole process.
  override lazy val hashCode: Int = scala.runtime.ScalaRunTime._hashCode(this)

  def withFieldNameMapper(m: NameMapper): DynamoDBCodecDeriverConfigure[A]          = copy(fieldNameMapper = m)
  def withCaseNameMapper(m: NameMapper): DynamoDBCodecDeriverConfigure[A]           = copy(caseNameMapper = m)
  def withDiscriminatorKind(k: DiscriminatorKind): DynamoDBCodecDeriverConfigure[A] = copy(discriminatorKind = k)
  def withEnumValuesAsStrings(b: Boolean): DynamoDBCodecDeriverConfigure[A]         = copy(enumValuesAsStrings = b)
  def withRejectExtraFields(b: Boolean): DynamoDBCodecDeriverConfigure[A]           = copy(rejectExtraFields = b)
  def withTransientNone(b: Boolean): DynamoDBCodecDeriverConfigure[A]               = copy(transientNone = b)
  def withRequireOptionFields(b: Boolean): DynamoDBCodecDeriverConfigure[A]         = copy(requireOptionFields = b)
  def withTransientEmptyCollection(b: Boolean): DynamoDBCodecDeriverConfigure[A]    = copy(transientEmptyCollection = b)
  def withRequiredCollectionFields(b: Boolean): DynamoDBCodecDeriverConfigure[A]    = copy(requireCollectionFields = b)
  def withTransientDefaultValue(b: Boolean): DynamoDBCodecDeriverConfigure[A]       = copy(transientDefaultValue = b)
  def withRequireDefaultValueFields(b: Boolean): DynamoDBCodecDeriverConfigure[A]   = copy(requireDefaultValueFields = b)

  def withSchema1TupleCompatibility(v: Schema1Compat): DynamoDBCodecDeriverConfigure[A]        =
    copy(schema1TupleCompat = v)
  def withSchema1ByteSequenceCompatibility(v: Schema1Compat): DynamoDBCodecDeriverConfigure[A] =
    copy(schema1ByteSequenceCompat = v)
  def withSchema1ByteCompatibility(v: Schema1Compat): DynamoDBCodecDeriverConfigure[A]         =
    copy(schema1ByteCompat = v)
  def withSchema1YearCompatibility(v: Schema1Compat): DynamoDBCodecDeriverConfigure[A]         =
    copy(schema1YearCompat = v)

  /** Records a per-field modifier — same name and signature as `Deriver.withModifier`. */
  def withModifier[T](typeId: TypeId[T], field: String, modifier: Modifier.Term): DynamoDBCodecDeriverConfigure[A] =
    copy(termModifiers = termModifiers :+ ((typeId.asInstanceOf[TypeId[Any]], field, modifier)))

  /** Records a per-type modifier — same name and signature as `Deriver.withModifier`. */
  def withModifier[T](typeId: TypeId[T], modifier: Modifier.Reflect): DynamoDBCodecDeriverConfigure[A] =
    copy(typeModifiers = typeModifiers :+ ((typeId.asInstanceOf[TypeId[Any]], modifier)))

  /** Records a codec-instance override — same as `Deriver.withInstance`. */
  def withInstance[T](instance: DynamoDBCodec[T])(implicit typeId: TypeId[T]): DynamoDBCodecDeriverConfigure[A] =
    copy(instanceOverrides =
      instanceOverrides :+ ((typeId.asInstanceOf[TypeId[Any]], instance.asInstanceOf[DynamoDBCodec[Any]]))
    )

  /** Fold this policy into a `Deriver[DynamoDBCodec]` for `Schema#deriving`. */
  def toDeriver: Deriver[DynamoDBCodec] = {
    val scalar = DynamoDBCodecDeriver
      .withFieldNameMapper(fieldNameMapper)
      .withCaseNameMapper(caseNameMapper)
      .withDiscriminatorKind(discriminatorKind)
      .withEnumValuesAsStrings(enumValuesAsStrings)
      .withRejectExtraFields(rejectExtraFields)
      .withTransientNone(transientNone)
      .withTransientEmptyCollection(transientEmptyCollection)
      .withRequiredCollectionFields(requireCollectionFields)
      .withTransientDefaultValue(transientDefaultValue)
      .withRequireDefaultValueFields(requireDefaultValueFields)
      .withSchema1TupleCompatibility(schema1TupleCompat)
      .withSchema1ByteSequenceCompatibility(schema1ByteSequenceCompat)
      .withSchema1ByteCompatibility(schema1ByteCompat)
      .withSchema1YearCompatibility(schema1YearCompat)
      .copy(requireOptionFields = requireOptionFields) // no `withXxx` setter for this one
    val withTypeMods = typeModifiers.foldLeft(scalar: Deriver[DynamoDBCodec]) { case (d, (typeId, modifier)) =>
      d.withModifier(typeId, modifier)
    }
    val withTermMods = termModifiers.foldLeft(withTypeMods) { case (d, (typeId, field, modifier)) =>
      d.withModifier(typeId, field, modifier)
    }
    instanceOverrides.foldLeft(withTermMods) { case (d, (typeId, instance)) =>
      d.withInstance(instance)(typeId)
    }
  }

  /**
   * Fold the naming-relevant subset of this policy into a `Deriver[Resolver]` for
   * `Schema#deriving` - see [[zio.dynamodb.blocks.schema.ResolverDeriver]]. None of the
   * encode/decode-behaviour settings (`enumValuesAsStrings`, `rejectExtraFields`, ...)
   * are threaded - they don't affect where an attribute lives. A type with a codec
   * `withInstance` override resolves as an opaque `Resolver.Leaf`: a hand-written codec's
   * wire shape can't be inferred from `Reflect`.
   */
  def toResolverDeriver: Deriver[Resolver] = {
    val scalar       = ResolverDeriver
      .withFieldNameMapper(fieldNameMapper)
      .withCaseNameMapper(caseNameMapper)
      .withDiscriminatorKind(discriminatorKind)
    val withTypeMods = typeModifiers.foldLeft(scalar: Deriver[Resolver]) { case (d, (typeId, modifier)) =>
      d.withModifier(typeId, modifier)
    }
    val withTermMods = termModifiers.foldLeft(withTypeMods) { case (d, (typeId, field, modifier)) =>
      d.withModifier(typeId, field, modifier)
    }
    instanceOverrides.foldLeft(withTermMods) { case (d, (typeId, _)) =>
      d.withInstance[Any](Resolver.Leaf[Any]())(typeId)
    }
  }
}

object DynamoDBCodecDeriverConfigure {

  private val Default: DynamoDBCodecDeriverConfigure[Any] = DynamoDBCodecDeriverConfigure[Any]()

  implicit def default[A]: DynamoDBCodecDeriverConfigure[A] =
    Default.asInstanceOf[DynamoDBCodecDeriverConfigure[A]]
}
