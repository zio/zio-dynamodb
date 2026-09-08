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
 * Resolves an optic path — a [[DynamicOptic]] of `Field` / `Case` / `AtIndex` / `AtMapKey`
 * nodes — to the DynamoDB attribute path that a `.where` / `.filter` / key-condition
 * expression must reference that field by on the wire.
 *
 * One instance per [[zio.dynamodb.blocks.ddbexpr.Table]]. Its `root: Resolver[A]` is a tree
 * mirroring `Schema[A]`, produced once at `Table` construction by
 * [[zio.dynamodb.blocks.schema.ResolverDeriver]] from that table's
 * `(Schema, DynamoDBCodecDeriverConfig)` pair, with every wire name already decided: the
 * field-name mapper, per-field `@Modifier.rename`, and discriminator kind are all applied at
 * derivation time, not on lookup.
 *
 * A lookup therefore walks `root` alongside the optic's nodes, appends one
 * [[ProjectionExpression]] segment per node, and reads the pre-baked wire name off the
 * matching `Resolver` node — no schema or config is re-examined per call. A path that cannot
 * be a DynamoDB attribute path (e.g. a non-`String` map key) resolves to a `Left` with a
 * message rather than a best-effort guess.
 *
 * This is the config-aware counterpart of [[OpticToPE]], which only ever sees an optic's raw
 * Scala field names; routing through `root` is what keeps a filtered or key field on the
 * same wire name as the item body a `put` writes.
 *
 * Lifecycle, in order:
 *
 *   1. At `Table` construction, `.derive` builds `root` but forces only its top node — the
 *      top-level record's own field → wire-name map. Every nested type's `Resolver` is
 *      captured as an unforced thunk.
 *   2. The first [[resolve]] that descends into a nested type forces that subtree's thunk
 *      (running its naming computation once) and memoises it; [[resolve]] also caches the
 *      finished [[ProjectionExpression]] chain, keyed by the optic.
 *   3. [[resolveTopLevelField]] — the shape every key optic has — skips both the walk and
 *      the cache: one `root.fields` lookup against the map from step 1.
 *
 * So steady state (a fixed set of keyed / filtered fields) does no derivation and no
 * attribute-path rebuilding.
 *
 * Two entry points: [[resolve]] for any optic path, [[resolveTopLevelField]] for the
 * single-top-level-`Field` key shape.
 */
private[blocks] final class ProjectionResolver[A](root: Resolver[A]) {

  // Keyed by DynamicOptic alone: `root` already fixes one (schema, config) pair per Table,
  // so the key needn't carry them the way OpticToPE's old (reflect, optic, config) key did.
  private[this] val cache = new ConcurrentHashMap[DynamicOptic, Either[String, ProjectionExpression[_, _]]]()

  /**
   * Resolves any optic path to its [[ProjectionExpression]]. Memoised:
   * [[ProjectionResolver.walk]] rebuilds a fresh `MapElement` / `ListElement` chain on every
   * call, so a deep or repeatedly-used path is worth caching.
   */
  def resolve(dyn: DynamicOptic): Either[String, ProjectionExpression[_, _]] = {
    val hit = cache.get(dyn)
    if (hit ne null) hit
    else {
      val computed = ProjectionResolver.walk(root, OpticToPE.pruneOptionalNodes(dyn.nodes))
      cache.putIfAbsent(dyn, computed)
      computed
    }
  }

  /**
   * Resolves a single top-level field's wire name directly — the shape every DynamoDB key
   * optic has. Reads the very same `root.fields` entry that [[ProjectionResolver.walk]] reads
   * for a `Field` node, so it cannot disagree with [[resolve]]: a shorter route to the same
   * answer, not a second naming rule.
   */
  // No ConcurrentHashMap and no walk here: `root.fields` is a small immutable Map (direct
  // key comparison, JIT-inlined) which beats a CHM.get, and a one-segment path has no
  // MapElement chain to memoise. Hit by every get / query / update / delete, so it earns
  // its own method.
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

private[blocks] object ProjectionResolver {

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
