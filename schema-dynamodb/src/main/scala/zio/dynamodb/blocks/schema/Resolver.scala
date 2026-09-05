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

import zio.blocks.schema.Lazy
import zio.blocks.schema.json.DiscriminatorKind

import scala.collection.immutable.{ Map => ScalaMap }

/**
 * Per-type naming information for resolving an optic path to a DynamoDB attribute path,
 * produced by [[ResolverDeriver]] alongside (but independently of) [[DynamoDBCodec]].
 * Mirrors [[zio.blocks.schema.Reflect]]'s shape (one case per `Deriver` method) but carries
 * only what path-resolution needs: wire names and, for recursion, a `Lazy` pointer to the
 * child type's own `Resolver` — no registers, constructors/deconstructors, or encode/decode.
 *
 * Bare case names, referenced qualified (`Resolver.Record`, `Resolver.Map`, ...) — the same
 * house style `AttributeValue.Map` uses to coexist with `scala.collection.Map` (see the
 * `ScalaMap` alias below).
 */
sealed trait Resolver[A]

object Resolver {

  /**
   * Primitives, `DynamicValue`, and any type given an instance override (`withInstance`) —
   *  a hand-written codec's internal wire shape can't be inferred from `Reflect`, so a path
   *  that tries to go further through one resolves to a `Left`, not a guess.
   */
  final case class Leaf[A]() extends Resolver[A]

  /** Scala field name -> (wire name, the field type's own `Resolver`, forced on descent). */
  final case class Record[A](fields: ScalaMap[String, (String, Lazy[Resolver[_]])]) extends Resolver[A]

  /**
   * Scala case name -> (wire name, the case type's own `Resolver`). The wire name is only
   *  used when `discriminatorKind` is `Key`; for `Field(_)` the case contributes no path
   *  segment, and for `None` a path through a variant can't be built at all.
   */
  final case class Variant[A](
    discriminatorKind: DiscriminatorKind,
    cases: ScalaMap[String, (String, Lazy[Resolver[_]])]
  ) extends Resolver[A]

  /**
   * `List[A]` / `Vector[A]` / etc. An index segment carries no name; still need the
   *  element type's `Resolver` for a path that continues deeper.
   */
  final case class Sequence[A](element: Lazy[Resolver[_]]) extends Resolver[A]

  /**
   * `Map[K, V]`. A map-key segment carries no name (the key string is itself the DynamoDB
   *  attribute name); still need the value type's `Resolver`.
   */
  final case class Map[A](value: Lazy[Resolver[_]]) extends Resolver[A]

  /**
   * An opaque / newtype wrapper. Transparent to path resolution — an optic sees straight
   *  through it, so walking a path derefs a `Wrapper` node without consuming a path
   *  segment, exactly like unwrapping `Reflect.Wrapper` did in `OpticToPE.deref`.
   */
  final case class Wrapper[A](inner: Lazy[Resolver[_]]) extends Resolver[A]
}
