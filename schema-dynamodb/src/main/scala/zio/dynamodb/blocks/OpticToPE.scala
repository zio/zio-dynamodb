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

import java.util.concurrent.ConcurrentHashMap
import zio.blocks.schema.{
  DynamicOptic,
  DynamicValue,
  Lens,
  Modifier,
  NameMapper,
  Optic,
  Optional,
  PrimitiveValue,
  Reflect
}
import zio.blocks.schema.binding.Binding
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.ProjectionExpression

object OpticToPE {

  def pe(dynamicOptic: DynamicOptic): Either[String, ProjectionExpression[_, _]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodes                              = dynamicOptic.nodes
    var idx                                = 0
    var error: String                      = null
    while (idx < nodes.length && error == null) {
      nodes(idx) match {
        case DynamicOptic.Node.Field(name) => prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                          => error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe)
  }

  def pe[S, A](optic: Optic[S, A]): Either[String, ProjectionExpression[S, A]] =
    optic match {
      case lens: Lens[S, A]         => pe(lens)
      case optional: Optional[S, A] => pe(optional)
      case _                        => Left(s"unexpected optic: $optic")
    }

  // Lens[S, A] implies no indexed access anywhere in the path — MapElements all the way down.
  def pe[S, A](lens: Lens[S, A]): Either[String, ProjectionExpression[S, A]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodes                              = lens.toDynamic.nodes
    var idx                                = 0
    var error: String                      = null
    while (idx < nodes.length && error == null) {
      nodes(idx) match {
        case DynamicOptic.Node.Field(name) => prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                          => error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe.asInstanceOf[ProjectionExpression[S, A]])
  }

  private[this] final def pruneOptionalNodes(nodes: IndexedSeq[DynamicOptic.Node]): IndexedSeq[DynamicOptic.Node] = {
    val builder = Vector.newBuilder[DynamicOptic.Node]
    var i       = 0
    while (i < nodes.length)
      if (
        i + 1 < nodes.length &&
        nodes(i) == DynamicOptic.Node.Case("Some") &&
        nodes(i + 1) == DynamicOptic.Node.Field("value")
      )
        i += 2 // skip both
      else {
        builder += nodes(i)
        i += 1
      }
    builder.result()
  }

  def pe[S, A](optional: Optional[S, A]): Either[String, ProjectionExpression[S, A]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodesPruned                        = pruneOptionalNodes(optional.toDynamic.nodes)
    var idx                                = 0
    var error: String                      = null
    while (idx < nodesPruned.length && error == null) {
      nodesPruned(idx) match {
        case DynamicOptic.Node.Field(name)                                                  =>
          prevPe = ProjectionExpression.MapElement(prevPe, name)
        case DynamicOptic.Node.AtIndex(index)                                               =>
          prevPe = ProjectionExpression.ListElement(prevPe, index)
        case DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String(key))) =>
          prevPe = ProjectionExpression.MapElement(prevPe, key)
        case DynamicOptic.Node.AtMapKey(key)                                                =>
          error = s"found map key '$key' — only String keys are supported in DDB"
        case DynamicOptic.Node.Case(name)                                                   =>
          prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                                                                           =>
          error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe.asInstanceOf[ProjectionExpression[S, A]])
  }

  // ── Config-aware resolution ────────────────────────────────────────────────
  //
  // Walks the optic's DynamicOptic and the type's Reflect tree in lockstep, resolving
  // each Field / Case segment to the DynamoDB attribute name the given
  // DynamoDBCodecDeriverConfigure produces — the value-level equivalent of what the
  // derived codec writes for the item body. Memoised by (root Reflect identity, optic,
  // config value). Everything here is public zio-blocks API (Reflect / Modifier /
  // NameMapper) plus the config case class — no deriver internals.

  private[this] final class PeKey(private val root: AnyRef, private val dyn: DynamicOptic, private val cfg: AnyRef) {
    override val hashCode: Int           = (System.identityHashCode(root) * 31 + dyn.hashCode) * 31 + cfg.hashCode
    override def equals(o: Any): Boolean = o match {
      case k: PeKey => (root eq k.root) && (dyn == k.dyn) && (cfg == k.cfg)
      case _        => false
    }
  }

  private[this] val peCache = new ConcurrentHashMap[PeKey, Either[String, ProjectionExpression[_, _]]]()

  def pe[S, A](
    optic: Optic[S, A],
    reflect: Reflect[Binding, S],
    config: zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure[_]
  ): Either[String, ProjectionExpression[S, A]] =
    peCache
      .computeIfAbsent(
        new PeKey(reflect, optic.toDynamic, config),
        _ => resolve(optic.toDynamic, reflect, config)
      )
      .asInstanceOf[Either[String, ProjectionExpression[S, A]]]

  def pe(
    dyn: DynamicOptic,
    reflect: Reflect[Binding, _],
    config: zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure[_]
  ): Either[String, ProjectionExpression[_, _]] =
    peCache.computeIfAbsent(new PeKey(reflect, dyn, config), _ => resolve(dyn, reflect, config))

  // Skip past the nodes that carry no attribute-path segment: `Deferred` is a lazily-forced
  // node used for recursive types, `Wrapper` is an opaque type / newtype around an inner
  // Reflect. After deref, `r` is a concrete Record / Variant / Sequence / Map / Primitive.
  @annotation.tailrec
  private[this] def deref(r: Reflect[Binding, _]): Reflect[Binding, _] = r match {
    case d: Reflect.Deferred[Binding, _] @unchecked   => deref(d.value)
    case w: Reflect.Wrapper[Binding, _, _] @unchecked => deref(w.wrapped)
    case other                                        => other
  }

  private[this] def fieldWireName(
    scalaName: String,
    modifiers: Seq[Modifier.Term],
    record: Reflect.Record[Binding, _],
    config: zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure[_]
  ): String = {
    val renamed =
      config.termModifiers.collectFirst {
        case (tid, f, Modifier.rename(w)) if f == scalaName && tid == record.typeId => w
      }.orElse(modifiers.collectFirst { case Modifier.rename(w) => w })
    renamed.getOrElse {
      val mapper = record.modifiers.collectFirst { case Modifier.fieldNaming(s) => NameMapper.fromString(s) }
        .getOrElse(config.fieldNameMapper)
      mapper(scalaName)
    }
  }

  private[this] def caseWireName(
    caseName: String,
    modifiers: Seq[Modifier.Term],
    variant: Reflect.Variant[Binding, _],
    config: zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure[_]
  ): String =
    modifiers.collectFirst { case Modifier.rename(w) => w }.getOrElse {
      val mapper = variant.modifiers.collectFirst { case Modifier.caseNaming(s) => NameMapper.fromString(s) }
        .getOrElse(config.caseNameMapper)
      mapper(caseName)
    }

  // Walks the optic's segments (`dyn.nodes`) and the type's structure (`rootReflect`) in
  // step: `pe` accumulates the DynamoDB attribute path, `cur` tracks the current position
  // in the schema so the next Field / Case segment can be resolved against its owning type.
  //   - Field / Case  -> renamed per `config` (fieldWireName / caseWireName), appended as a
  //                      MapElement
  //   - AtIndex        -> ListElement, passed through unchanged
  //   - String AtMapKey-> MapElement, passed through unchanged
  //   - a non-String map key, or a segment that doesn't match the schema shape -> Left
  // Memoised by the caller (see the `pe` overload above), so this runs once per
  // (root type, optic, config).
  private[this] def resolve(
    dyn: DynamicOptic,
    rootReflect: Reflect[Binding, _],
    config: zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure[_]
  ): Either[String, ProjectionExpression[_, _]] = {
    val nodes                          = pruneOptionalNodes(dyn.nodes)
    var pe: ProjectionExpression[_, _] = ProjectionExpression.Root
    var cur: Reflect[Binding, _]       = deref(rootReflect)
    var err: String                    = null
    var i                              = 0
    while (i < nodes.length && err == null) {
      nodes(i) match {
        case DynamicOptic.Node.Field(scalaName)                                           =>
          cur match {
            case rec: Reflect.Record[Binding, _] @unchecked =>
              rec.fields.find(_.name == scalaName) match {
                case Some(term) =>
                  pe = ProjectionExpression.MapElement(pe, fieldWireName(scalaName, term.modifiers, rec, config))
                  cur = deref(term.value)
                case _          => err = s"field '$scalaName' not found in ${rec.typeId.name}"
              }
            case _                                          => err = s"path segment '$scalaName' is not a record field"
          }
        case DynamicOptic.Node.Case(caseName)                                             =>
          cur match {
            case v: Reflect.Variant[Binding, _] @unchecked =>
              v.cases.find(_.name == caseName) match {
                case Some(caseTerm) =>
                  config.discriminatorKind match {
                    case DiscriminatorKind.Key      =>
                      pe = ProjectionExpression.MapElement(pe, caseWireName(caseName, caseTerm.modifiers, v, config))
                      cur = deref(caseTerm.value)
                    case DiscriminatorKind.Field(_) =>
                      cur = deref(caseTerm.value) // discriminator is a sibling field; the case adds no segment
                    case DiscriminatorKind.None =>
                      err = "cannot build a path through a variant case with DiscriminatorKind.None"
                  }
                case _              => err = s"case '$caseName' not found in ${v.typeId.name}"
              }
            case _                                         => err = s"path segment 'case $caseName' is not a variant"
          }
        case DynamicOptic.Node.AtIndex(idx)                                               =>
          pe = ProjectionExpression.ListElement(pe, idx)
          cur = cur match { case s: Reflect.Sequence[Binding, _, _] @unchecked => deref(s.element); case o => o }
        case DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String(k))) =>
          pe = ProjectionExpression.MapElement(pe, k)
          cur = cur match { case m: Reflect.Map[Binding, _, _, _] @unchecked => deref(m.value); case o => o }
        case DynamicOptic.Node.AtMapKey(k)                                                =>
          err = s"found map key '$k' — only String keys are supported in DDB"
        case node                                                                         =>
          err = s"unexpected optic node: $node"
      }
      i += 1
    }
    if (err != null) Left(err) else Right(pe)
  }

}
