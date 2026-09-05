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
import zio.blocks.schema.{ DynamicOptic, DynamicValue, PrimitiveValue }
import zio.blocks.schema.json.DiscriminatorKind
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.blocks.schema.Resolver

/**
 * Per-`Table` cache over a [[Resolver]][A] root, resolving an optic's [[DynamicOptic]] path
 * to a [[ProjectionExpression]]. The naming decision is already baked into `root` at
 * derivation time, so a lookup is a handful of `Map.get`s, not a schema-plus-config
 * re-derivation.
 *
 * Keyed by `DynamicOptic` alone (not `(reflect, optic, config)` the way `OpticToPE`'s cache
 * was) - `root` already bakes in one specific `(schema, config)` pair, one per `Table`, so
 * the key doesn't need to distinguish them.
 */
final class ProjectionResolver[A](root: Resolver[A]) {

  private[this] val cache = new ConcurrentHashMap[DynamicOptic, Either[String, ProjectionExpression[_, _]]]()

  def resolve(dyn: DynamicOptic): Either[String, ProjectionExpression[_, _]] = {
    val hit = cache.get(dyn)
    if (hit ne null) hit
    else {
      val computed = ProjectionResolver.walk(root, OpticToPE.pruneOptionalNodes(dyn.nodes))
      cache.putIfAbsent(dyn, computed)
      computed
    }
  }

  // Fast path for a single top-level field - the only shape a DynamoDB key optic can be.
  // Reads the same `root.fields` map general resolution does (so it can't disagree with it -
  // same source, not a second naming computation), skipping both the ConcurrentHashMap cache
  // and the general walk: a hash-map lookup costs more than a direct small-immutable-Map
  // lookup, and on the single-segment key path there is nothing to memoise (no chain of
  // MapElements to avoid rebuilding).
  def resolveTopLevelField(scalaName: String): Either[String, String] =
    root match {
      case r: Resolver.Record[_] @unchecked =>
        r.fields.get(scalaName) match {
          case Some((wireName, _)) => Right(wireName)
          case None                => Left(s"field '$scalaName' not found")
        }
      case _                                => Left(s"root is not a record")
    }
}

object ProjectionResolver {

  // Wrapper is transparent to path resolution - an optic sees straight through an opaque /
  // newtype wrapper, so a path never carries a node for it.
  @annotation.tailrec
  private[blocks] def deref(r: Resolver[_]): Resolver[_] = r match {
    case w: Resolver.Wrapper[_] @unchecked => deref(w.inner.force)
    case other                             => other
  }

  // Walks already-pruned nodes (see OpticToPE.pruneOptionalNodes) against a Resolver tree,
  // mirroring OpticToPE.resolve's control flow but reading pre-baked wire names off
  // `Resolver` instead of re-deriving them from raw Reflect + config on every call.
  private[blocks] def walk(
    root: Resolver[_],
    nodes: IndexedSeq[DynamicOptic.Node]
  ): Either[String, ProjectionExpression[_, _]] = {
    var pe: ProjectionExpression[_, _] = ProjectionExpression.Root
    var cur: Resolver[_]               = deref(root)
    var err: String                    = null
    var i                              = 0
    while (i < nodes.length && err == null) {
      nodes(i) match {
        case DynamicOptic.Node.Field(scalaName) =>
          cur match {
            case r: Resolver.Record[_] @unchecked =>
              r.fields.get(scalaName) match {
                case Some((wireName, child)) =>
                  pe = ProjectionExpression.MapElement(pe, wireName)
                  cur = deref(child.force)
                case None                    =>
                  err = s"field '$scalaName' not found"
              }
            case _                                => err = s"path segment '$scalaName' is not a record field"
          }

        case DynamicOptic.Node.Case(caseName) =>
          cur match {
            case v: Resolver.Variant[_] @unchecked =>
              v.cases.get(caseName) match {
                case Some((wireName, child)) =>
                  v.discriminatorKind match {
                    case DiscriminatorKind.Key      =>
                      pe = ProjectionExpression.MapElement(pe, wireName)
                      cur = deref(child.force)
                    case DiscriminatorKind.Field(_) =>
                      cur = deref(child.force) // discriminator is a sibling field; the case adds no segment
                    case DiscriminatorKind.None =>
                      err = "cannot build a path through a variant case with DiscriminatorKind.None"
                  }
                case None                    => err = s"case '$caseName' not found"
              }
            case _                                 => err = s"path segment 'case $caseName' is not a variant"
          }

        case DynamicOptic.Node.AtIndex(idx) =>
          cur match {
            case s: Resolver.Sequence[_] @unchecked =>
              pe = ProjectionExpression.ListElement(pe, idx)
              cur = deref(s.element.force)
            case _                                  => err = s"'$idx' is not a sequence index"
          }

        case DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String(k))) =>
          cur match {
            case m: Resolver.Map[_] @unchecked =>
              pe = ProjectionExpression.MapElement(pe, k)
              cur = deref(m.value.force)
            case _                             => err = s"'$k' is not a map key"
          }

        case DynamicOptic.Node.AtMapKey(k) =>
          err = s"found map key '$k' — only String keys are supported in DDB"

        case node =>
          err = s"unexpected optic node: $node"
      }
      i += 1
    }
    if (err != null) Left(err) else Right(pe)
  }
}
