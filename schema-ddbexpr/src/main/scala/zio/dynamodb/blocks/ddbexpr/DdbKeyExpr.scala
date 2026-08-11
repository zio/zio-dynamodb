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

package zio.dynamodb.blocks.ddbexpr

import zio.blocks.schema.Optic
import zio.dynamodb.blocks.schema.DynamoDBCodec

/**
 * Typed key condition expression ADT for DynamoDB query/get/delete/update operations.
 *
 *  Mirrors the three-case [[zio.dynamodb.KeyConditionExpr]] hierarchy but uses
 *  [[zio.blocks.schema.Optic]] for field references and carries [[DynamoDBCodec]][A]
 *  at every literal site — the same codec-carrying principle as [[DdbExpr.Lit]].
 *
 *  The ADT has two levels:
 *   - [[DdbKeyExpr.PrimaryKey]] — sealed sub-trait covering [[DdbKeyExpr.PartitionKeyEquals]] and
 *     [[DdbKeyExpr.Composite]]; mirrors [[zio.dynamodb.KeyConditionExpr.PrimaryKeyExpr]];
 *     accepted by `get`, `update`, and `deleteFrom`.
 *     Passing a range expression to these operations is a compile-time error.
 *   - [[DdbKeyExpr]] (full ADT) — also includes [[DdbKeyExpr.Extended]] (range/function
 *     sort key); accepted by `query` and the implicit `.whereKey` conversion.
 *
 *  Construction (import `DdbKeyExpr._` for codec derivation and extension methods).
 *  `.partitionKey`/`.sortKey` match the naming used by the LL API's
 *  [[zio.dynamodb.ProjectionExpression.partitionKey]]/[[zio.dynamodb.ProjectionExpression.sortKey]]:
 *  {{{
 *    import DdbKeyExpr._
 *
 *    // partition key only
 *    Task.id.partitionKey === "alice"
 *
 *    // partition key + sort key equality
 *    Task.id.partitionKey === "alice" && Task.score.sortKey === 42
 *
 *    // partition key + sort key range / function (query only — compile error on get/update/deleteFrom)
 *    Task.id.partitionKey === "alice" && Task.score.sortKey > 10
 *    Task.id.partitionKey === "alice" && Task.score.sortKey.between(10, 100)
 *    Task.id.partitionKey === "alice" && Task.name.sortKey.beginsWith("prefix")
 *  }}}
 *
 *  Interpretation to [[zio.dynamodb.KeyConditionExpr]] is handled by
 *  [[DdbKeyExprInterpreter]].
 */
sealed trait DdbKeyExpr[S]

/**
 * Implicit syntax for [[DdbKeyExpr]], extracted from `object DdbKeyExpr` into a mixin-able
 *  trait so the [[dsl]] facade can combine it with [[DdbExprSyntax]]/[[DdbExprApiSyntax]]
 *  under a single import. `object DdbKeyExpr extends DdbKeyExprSyntax` below is unaffected —
 *  every member here remains reachable as `DdbKeyExpr.XXX` exactly as before.
 *
 *  The ADT nodes and builder classes (`PartitionKeyEquals`, `PartitionKeyBuilder`, etc.) stay
 *  declared directly in `object DdbKeyExpr`, not here — see the equivalent note on
 *  [[DdbExprSyntax]] for why (path-dependent outer references on case classes nested in a
 *  trait mixed into more than one object).
 */
trait DdbKeyExprSyntax extends DerivedCodecSyntax {

  // ── Lens extension methods ─────────────────────────────────────────────────

  // Following the ZB query-dsl-extending guide pattern: extension methods on
  // Optic return builders that provide operator syntax. Naming matches the LL API's
  // ProjectionExpression.partitionKey/.sortKey:
  //   Task.id.partitionKey === "alice"                                   (partition key only)
  //   Task.id.partitionKey === "alice" && Task.score.sortKey === 42      (composite)
  //   Task.id.partitionKey === "alice" && Task.score.sortKey > 10        (extended)
  // The apply() method on PartitionKeyBuilder also preserves:
  //   Task.id.partitionKey("alice")     (call-style, backward compatible)
  // Not `extends AnyVal`: value classes may only be top-level or object members, not
  // trait members, and this trait is mixed into more than one object (DdbKeyExpr, dsl).
  implicit class LensKeyOps[S, A](private val optic: Optic[S, A]) {
    def partitionKey: DdbKeyExpr.PartitionKeyBuilder[S, A] = new DdbKeyExpr.PartitionKeyBuilder(optic)
    def sortKey: DdbKeyExpr.SortKeyBuilder[S, A]           = new DdbKeyExpr.SortKeyBuilder(optic)
  }

  // String-specific sort key ops — resolved via implicit class when B =:= String.
  implicit class SortKeyBuilderStrOps[S](private val b: DdbKeyExpr.SortKeyBuilder[S, String]) {
    def beginsWith(prefix: String): DdbKeyExpr.SortKeyExtended[S] =
      DdbKeyExpr.SortKeyExtended.BeginsWith(b.optic, prefix)
  }
}

object DdbKeyExpr extends DdbKeyExprSyntax {

  // ── ADT nodes ──────────────────────────────────────────────────────────────
  // Declared directly in this singleton object (not in DdbKeyExprSyntax above) so they stay
  // path-independent — see the note on DdbKeyExprSyntax for why.

  /**
   * Sealed sub-trait for primary-key expressions (partition-key-only or
   *  partition-key+sort-key-equality).
   *  Mirrors [[zio.dynamodb.KeyConditionExpr.PrimaryKeyExpr]].
   *  Accepted by `get`, `update`, and `deleteFrom` — operations that address a
   *  single item. Range expressions ([[Extended]]) do not extend this trait, so
   *  passing `sortKey > value` or `sortKey.between(...)` to those operations is a
   *  compile-time error.
   */
  sealed trait PrimaryKey[S] extends DdbKeyExpr[S]

  final case class PartitionKeyEquals[S, A](optic: Optic[S, A], value: A, codec: DynamoDBCodec[A])
      extends PrimaryKey[S] {
    def &&[B](sortKey: SortKeyEquals[S, B]): Composite[S, A, B] = Composite(this, sortKey)
    def &&(sortKey: SortKeyExtended[S]): Extended[S, A]         = Extended(this, sortKey)
  }

  final case class Composite[S, A, B](pk: PartitionKeyEquals[S, A], sk: SortKeyEquals[S, B]) extends PrimaryKey[S]
  final case class Extended[S, A](pk: PartitionKeyEquals[S, A], sk: SortKeyExtended[S])      extends DdbKeyExpr[S]

  // ── Sort key intermediates ─────────────────────────────────────────────────

  final case class SortKeyEquals[S, B](optic: Optic[S, B], value: B, codec: DynamoDBCodec[B])

  sealed trait SortKeyExtended[S]
  object SortKeyExtended {
    final case class Gt[S, B](optic: Optic[S, B], value: B, codec: DynamoDBCodec[B])          extends SortKeyExtended[S]
    final case class Gte[S, B](optic: Optic[S, B], value: B, codec: DynamoDBCodec[B])         extends SortKeyExtended[S]
    final case class Lt[S, B](optic: Optic[S, B], value: B, codec: DynamoDBCodec[B])          extends SortKeyExtended[S]
    final case class Lte[S, B](optic: Optic[S, B], value: B, codec: DynamoDBCodec[B])         extends SortKeyExtended[S]
    final case class Between[S, B](optic: Optic[S, B], lo: B, hi: B, codec: DynamoDBCodec[B]) extends SortKeyExtended[S]
    final case class BeginsWith[S](optic: Optic[S, String], prefix: String)                   extends SortKeyExtended[S]
  }

  // ── Partition key builder ─────────────────────────────────────────────────

  // Returned by .partitionKey and the standalone partitionKey() factory. Provides
  // === for the operator-style  Task.id.partitionKey === "alice"  and apply() so the
  // previous call-style  Task.id.partitionKey("alice")  continues to compile unchanged.
  final class PartitionKeyBuilder[S, A](val optic: Optic[S, A]) {
    def ===(value: A)(implicit codec: DynamoDBCodec[A]): PartitionKeyEquals[S, A]   =
      PartitionKeyEquals(optic, value, codec)
    def apply(value: A)(implicit codec: DynamoDBCodec[A]): PartitionKeyEquals[S, A] =
      PartitionKeyEquals(optic, value, codec)
  }

  // ── Standalone factories ───────────────────────────────────────────────────

  def partitionKey[S, A](optic: Optic[S, A]): PartitionKeyBuilder[S, A] = new PartitionKeyBuilder(optic)

  def sortKey[S, B](optic: Optic[S, B]): SortKeyBuilder[S, B] = new SortKeyBuilder(optic)

  // ── Sort key builder ──────────────────────────────────────────────────────

  final class SortKeyBuilder[S, B](val optic: Optic[S, B]) {
    def ===(value: B)(implicit codec: DynamoDBCodec[B]): SortKeyEquals[S, B]        = SortKeyEquals(optic, value, codec)
    def >(value: B)(implicit codec: DynamoDBCodec[B]): SortKeyExtended[S]           = SortKeyExtended.Gt(optic, value, codec)
    def >=(value: B)(implicit codec: DynamoDBCodec[B]): SortKeyExtended[S]          = SortKeyExtended.Gte(optic, value, codec)
    def <(value: B)(implicit codec: DynamoDBCodec[B]): SortKeyExtended[S]           = SortKeyExtended.Lt(optic, value, codec)
    def <=(value: B)(implicit codec: DynamoDBCodec[B]): SortKeyExtended[S]          = SortKeyExtended.Lte(optic, value, codec)
    def between(lo: B, hi: B)(implicit codec: DynamoDBCodec[B]): SortKeyExtended[S] =
      SortKeyExtended.Between(optic, lo, hi, codec)
  }
}
