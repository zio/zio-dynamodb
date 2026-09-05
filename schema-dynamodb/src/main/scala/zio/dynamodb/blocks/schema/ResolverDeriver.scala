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

package zio.dynamodb.blocks.schema

import zio.blocks.docs.Doc
import zio.blocks.schema._
import zio.blocks.schema.binding._
import zio.blocks.schema.derive.Deriver
import zio.blocks.schema.json.DiscriminatorKind
import zio.blocks.typeid.TypeId

/**
 * Derives a [[Resolver]][A] for `A` — a second, independent `Deriver[Resolver]`, run via
 * `schema.deriving(resolverDeriver).derive` alongside (never instead of, and never sharing
 * code or mutable state with) [[DynamoDBCodecDeriver]]. The codec deriver's per-field naming
 * precedence already sees fully-merged modifiers (zio-blocks' `DerivationBuilder` prepends
 * `.withModifier(...)` overrides before calling in), so this deriver reads that same merged
 * `modifiers` sequence rather than re-deriving the precedence rule against the raw,
 * unmodified schema.
 *
 * Only the three naming-relevant settings are threaded (`fieldNameMapper`, `caseNameMapper`,
 * `discriminatorKind`) — none of `DynamoDBCodecDeriverConfigure`'s encode/decode-behaviour
 * settings (`enumValuesAsStrings`, `rejectExtraFields`, ...) affect where an attribute
 * lives, so this deriver has no use for them.
 */
case class ResolverDeriver(
  fieldNameMapper: NameMapper,
  caseNameMapper: NameMapper,
  discriminatorKind: DiscriminatorKind
) extends Deriver[Resolver] {

  def withFieldNameMapper(m: NameMapper): ResolverDeriver          = copy(fieldNameMapper = m)
  def withCaseNameMapper(m: NameMapper): ResolverDeriver           = copy(caseNameMapper = m)
  def withDiscriminatorKind(k: DiscriminatorKind): ResolverDeriver = copy(discriminatorKind = k)

  // Primitives and DynamicValue are leaves - nothing further to resolve through.
  def derivePrimitive[A](
    primitiveType: PrimitiveType[A],
    typeId: TypeId[A],
    binding: Binding.Primitive[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  ): Lazy[Resolver[A]] = Lazy(Resolver.Leaf())

  def deriveDynamic[F[_, _]](
    binding: Binding.Dynamic,
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[DynamicValue],
    examples: Seq[DynamicValue]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[DynamicValue]] = Lazy(Resolver.Leaf())

  def deriveRecord[F[_, _], A](
    fields: IndexedSeq[Term[F, A, ?]],
    typeId: TypeId[A],
    binding: Binding.Record[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[A]] = Lazy {
    // @Modifier.fieldNaming overrides the deriver-level fieldNameMapper for this type -
    // same precedence DynamoDBCodecDeriver applies to the (already-merged) modifiers.
    var effectiveFieldNameMapper: NameMapper = null
    modifiers.foreach {
      case m: Modifier.fieldNaming =>
        if (effectiveFieldNameMapper eq null) effectiveFieldNameMapper = NameMapper.fromString(m.strategy)
      case _                       =>
    }
    if (effectiveFieldNameMapper eq null) effectiveFieldNameMapper = fieldNameMapper

    val entries = fields.map { field =>
      var name: String = null
      field.modifiers.foreach {
        case m: Modifier.rename => if (name eq null) name = m.name
        case _                  =>
      }
      if (name eq null) name = effectiveFieldNameMapper(field.name)
      field.name -> (name, D.instance(field.value.metadata))
    }
    Resolver.Record(entries.toMap)
  }

  def deriveVariant[F[_, _], A](
    cases: IndexedSeq[Term[F, A, ?]],
    typeId: TypeId[A],
    binding: Binding.Variant[A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[A]] = Lazy {
    // @Modifier.caseNaming / @Modifier.discriminator override the deriver-level settings
    // for this type - same precedence DynamoDBCodecDeriver applies.
    var resolvedCaseNameMapper: NameMapper           = null
    var resolvedDiscriminatorKind: DiscriminatorKind = null
    modifiers.foreach {
      case m: Modifier.caseNaming    =>
        if (resolvedCaseNameMapper eq null) resolvedCaseNameMapper = NameMapper.fromString(m.strategy)
      case m: Modifier.discriminator =>
        if (resolvedDiscriminatorKind eq null) resolvedDiscriminatorKind = DiscriminatorKind.Field(m.name)
      case _                         =>
    }
    if (resolvedCaseNameMapper eq null) resolvedCaseNameMapper = caseNameMapper
    if (resolvedDiscriminatorKind eq null) resolvedDiscriminatorKind = discriminatorKind

    val entries = cases.map { case_ =>
      var name: String = null
      case_.modifiers.foreach {
        case m: Modifier.rename => if (name eq null) name = m.name
        case _                  =>
      }
      if (name eq null) name = resolvedCaseNameMapper(case_.name)
      case_.name -> (name, D.instance(case_.value.metadata))
    }
    Resolver.Variant(resolvedDiscriminatorKind, entries.toMap)
  }

  def deriveSequence[F[_, _], C[_], A](
    element: Reflect[F, A],
    typeId: TypeId[C[A]],
    binding: Binding.Seq[C, A],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[C[A]],
    examples: Seq[C[A]]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[C[A]]] =
    Lazy(Resolver.Sequence(D.instance(element.metadata)))

  def deriveMap[F[_, _], M[_, _], K, V](
    key: Reflect[F, K],
    value: Reflect[F, V],
    typeId: TypeId[M[K, V]],
    binding: Binding.Map[M, K, V],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[M[K, V]],
    examples: Seq[M[K, V]]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[M[K, V]]] =
    Lazy(Resolver.Map(D.instance(value.metadata)))

  def deriveWrapper[F[_, _], A, B](
    wrapped: Reflect[F, B],
    typeId: TypeId[A],
    binding: Binding.Wrapper[A, B],
    doc: Doc,
    modifiers: Seq[Modifier.Reflect],
    defaultValue: Option[A],
    examples: Seq[A]
  )(implicit F: HasBinding[F], D: HasInstance[F]): Lazy[Resolver[A]] =
    Lazy(Resolver.Wrapper(D.instance(wrapped.metadata)))
}

object ResolverDeriver
    extends ResolverDeriver(
      fieldNameMapper = NameMapper.Identity,
      caseNameMapper = NameMapper.Identity,
      discriminatorKind = DiscriminatorKind.Key
    )
