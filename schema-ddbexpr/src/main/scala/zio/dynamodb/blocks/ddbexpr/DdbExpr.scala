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

import zio.blocks.schema.{ DynamicSchemaExpr, Optic, Schema, SchemaExpr }
import zio.blocks.schema.comptime.Allows
import Allows.Wrapped
import zio.dynamodb.blocks.DdbGrammar
import zio.dynamodb.compat.||

import scala.annotation.unused
import scala.language.implicitConversions

/**
 * Typed DDB condition expression ADT.
 *
 *  Scalar comparisons and sealed-trait equality use ZB `Optic`
 *  operators directly (`===`, `>`, `<`, etc.), which produce `SchemaExpr[S, Boolean]`;
 *  [[DdbExprSyntax.schemaExprToDdbExpr]] lifts them into a `DdbExpr`. Literal values are
 *  encoded with the table's codec, so a sealed-trait value compares as `put` writes it.
 *
 *  DDB-specific functions ([[DdbExprSyntax.OpticDdbExprOps.attributeExists]],
 *  [[DdbExprSyntax.OpticDdbExprOps.between]], etc.) are extension methods on
 *  `Optic` and produce [[DdbExpr]] nodes directly.
 *
 *  Usage:
 *  {{{
 *    import DdbExpr._
 *
 *    // scalars and sealed traits — ZB Optic operators, lifted to Builtin
 *    Task.score > 0
 *    Task.name === "alice"
 *    Task.priority === Priority.High
 *
 *    // DDB functions
 *    Task.name.beginsWith("prefix")
 *    Task.name.contains("needle")
 *    Task.id.attributeExists
 *    Task.score.between(1, 100)
 *    Task.id.in("a", "b")
 *
 *    // logical combinators — ordering unrestricted since ZB v0.0.47+
 *    Task.score > 0 && Task.priority === Priority.High
 *    Task.score > 0 && Task.id.attributeExists
 *    Task.name.beginsWith("p") || Task.id.attributeExists
 *    !(Task.name === "bob")
 *  }}}
 */
sealed trait DdbExpr[S, A]

/**
 * Implicit syntax for [[DdbExpr]], extracted from `object DdbExpr` into a mixin-able trait so
 *  the [[dsl]] facade can combine it with [[DdbKeyExprSyntax]]/[[DdbExprApiSyntax]] under a
 *  single import. `object DdbExpr extends DdbExprSyntax` below is unaffected — every member
 *  here remains reachable as `DdbExpr.XXX` exactly as before.
 *
 *  The [[DdbExpr]] ADT nodes themselves (`Builtin`, `AttributeExists`, `And`, etc.) stay
 *  declared directly in `object DdbExpr`, not here: a case class nested in a `trait` picks up
 *  a path-dependent outer reference (only resolvable when there is a single fixed instance),
 *  which breaks once two objects — `DdbExpr` and `dsl` — both mix in this trait and could each
 *  construct one. Nesting them in the singleton `object` instead avoids that entirely, at the
 *  cost of this trait's methods spelling out `DdbExpr.AttributeExists(...)` instead of the
 *  unqualified `AttributeExists(...)` they could use when everything lived in one object.
 */
trait DdbExprSyntax {

  // ── Optic extension methods ─────────────────────────────────────────────────

  // DDB-specific ops for any Optic[S, A]. ZB's Optic already provides ===, >,
  // <, >= etc. as direct methods returning SchemaExpr; we add DDB functions here.
  // Not `extends AnyVal`: value classes may only be top-level or object members, not
  // trait members, and this trait is mixed into more than one object (DdbExpr, dsl).
  implicit class OpticDdbExprOps[S, A](private val optic: Optic[S, A]) {
    def attributeExists: DdbExpr[S, Boolean]    = DdbExpr.AttributeExists(optic)
    def attributeNotExists: DdbExpr[S, Boolean] =
      DdbExpr.AttributeNotExists(optic)
    def between(lo: A, hi: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.N || DdbGrammar.S || DdbGrammar.B]
    ): DdbExpr[S, Boolean]                      =
      DdbExpr.Between(optic, lo, hi, schema)

    /** `path IN (values)`, for a field of any type. */
    def in(head: A, rest: A*)(implicit schema: Schema[A]): DdbExpr[S, Boolean] =
      DdbExpr.In(optic, head +: rest, schema)

    /** Like [[in]], taking a `Set`; number, string and binary fields only, checked at compile time. */
    def inSet(values: Set[A])(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.N || DdbGrammar.S || DdbGrammar.B]
    ): DdbExpr[S, Boolean] =
      DdbExpr.In(optic, values.toSeq, schema)

    /** `contains(path, element)` on a set field: true when the set has `element`. */
    def containsElement[B](element: B)(implicit
      elemSchema: Schema[B],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): DdbExpr[S, Boolean] =
      DdbExpr.ContainsElement(optic, element, elemSchema)

    /** `size(path)`, to compare with `===`, `<`, `between`, ...; for string, binary, list, map and set fields. */
    def size(implicit
      @unused ev: Allows[
        A,
        DdbGrammar.S || DdbGrammar.B || DdbGrammar.L || DdbGrammar.M || DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS
      ]
    ): DdbExpr.Size[S, A] =
      DdbExpr.Size(optic)
  }

  // String-specific DDB functions.
  implicit class OpticStringDdbExprOps[S](private val optic: Optic[S, String]) {
    def contains(value: String): DdbExpr[S, Boolean]    = DdbExpr.Contains(optic, value)
    def beginsWith(prefix: String): DdbExpr[S, Boolean] = DdbExpr.BeginsWith(optic, prefix)
  }

  // ── Logical combinators ─────────────────────────────────────────────────────

  implicit class DdbExprBoolSyntax[S](val self: DdbExpr[S, Boolean]) {
    def &&(rhs: DdbExpr[S, Boolean]): DdbExpr[S, Boolean] = DdbExpr.And(self, rhs)
    def ||(rhs: DdbExpr[S, Boolean]): DdbExpr[S, Boolean] = DdbExpr.Or(self, rhs)
    def unary_! : DdbExpr[S, Boolean]                     = DdbExpr.Not(self)
  }

  // Bridge: SchemaExpr → DdbExpr for !, &&, ||.
  // !SchemaExpr: SchemaExpr has no unary_! of its own, so this class provides it.
  // &&/||: ZB v0.0.47+ moved &&/|| off SchemaExpr onto the BooleanOps companion implicit
  // class, so under Scala 2, SchemaExpr && DdbExpr needs this explicit bridge to resolve.
  implicit class SchemaExprBoolBridge[S](val self: SchemaExpr[S, Boolean]) {
    def unary_! : DdbExpr[S, Boolean]                     = DdbExpr.Not(DdbExpr.Builtin(self))
    def &&(rhs: DdbExpr[S, Boolean]): DdbExpr[S, Boolean] = DdbExpr.And(DdbExpr.Builtin(self), rhs)
    def ||(rhs: DdbExpr[S, Boolean]): DdbExpr[S, Boolean] = DdbExpr.Or(DdbExpr.Builtin(self), rhs)
  }

  // ── Implicit lift ───────────────────────────────────────────────────────────

  // Lifts SchemaExpr[S, Boolean] → DdbExpr[S, Boolean].
  // Enables: val expr: DdbExpr[S, Boolean] = Task.score > 0
  // and passes SchemaExpr where DdbExpr is expected (e.g. as DdbExprBoolSyntax.&& RHS).
  implicit def schemaExprToDdbExpr[S](se: SchemaExpr[S, Boolean]): DdbExpr[S, Boolean] =
    DdbExpr.Builtin(se)

  // ── Update expression builder ───────────────────────────────────────────────

  // Extension methods on Optic[From, A] that produce DdbUpdateExpr nodes — the deferred,
  // Schema-carrying counterpart of the condition ops above. Operand values are NOT encoded
  // here: each node holds a Schema[A], and DdbUpdateExprInterpreter (via DdbExprApi.update)
  // encodes them — and resolves attribute paths — against the originating Table's deriver
  // configuration, so `update` writes the same names/encodings as `put` does for the body.
  //
  // Allows[A, ...] constraints mirror schema-expr's LensUpdateExprSyntax — they are
  // orthogonal to encoding and prevent operations from being applied to fields of
  // incompatible DynamoDB types at compile time.
  //
  // Import: `import DdbExpr._` (same import as condition expression ops).
  implicit class OpticUpdateOps[From, A](private val optic: Optic[From, A]) {

    /** `SET path = value`. */
    def set(value: A)(implicit schema: Schema[A]): DdbUpdateExpr[From] =
      DdbUpdateExpr.SetValue(optic, value, schema)

    /** `SET path = other`: copies another attribute's value. */
    def set(other: Optic[From, A]): DdbUpdateExpr[From] =
      DdbUpdateExpr.SetPath(optic, other)

    /** `SET path = if_not_exists(path, value)`: sets the attribute only when it is absent. */
    def setIfNotExists(value: A)(implicit schema: Schema[A]): DdbUpdateExpr[From] =
      DdbUpdateExpr.SetIfNotExists(optic, value, schema)

    /** `REMOVE path`. */
    def remove: DdbUpdateExpr[From] = DdbUpdateExpr.Remove(optic)

    /** `REMOVE path[index]`, for list fields. */
    def remove(index: Int)(implicit @unused ev: Allows[A, DdbGrammar.L]): DdbUpdateExpr[From] =
      DdbUpdateExpr.RemoveAt(optic, index)

    /**
     * `SET path = path + delta`, for numeric fields. Fails if the attribute is absent - use [[add]] to
     * start from zero.
     */
    def increment(delta: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.Increment(optic, delta, schema)

    /** `SET path = path - delta`, for numeric fields. Fails if the attribute is absent, like [[increment]]. */
    def decrement(delta: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.Decrement(optic, delta, schema)

    /**
     * `ADD path value`, for numeric fields: an absent attribute counts as zero, so it is created.
     * For set union see [[addSet]].
     */
    def add(value: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.Add(optic, value, schema)

    /** `ADD path set`: adds the elements to a set field. */
    def addSet(value: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.AddToSet(optic, value, schema)

    /** `DELETE path set`: removes the elements from a set field. */
    def deleteFromSet(value: A)(implicit
      schema: Schema[A],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.DeleteFromSet(optic, value, schema)

    /** `SET path = list_append(path, items)`, for list fields. */
    def appendList[B](items: Seq[B])(implicit
      elemSchema: Schema[B],
      @unused ev: Allows[A, DdbGrammar.L]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.AppendList(optic, items, elemSchema)

    /** `SET path = list_append(items, path)`, for list fields. */
    def prependList[B](items: Seq[B])(implicit
      elemSchema: Schema[B],
      @unused ev: Allows[A, DdbGrammar.L]
    ): DdbUpdateExpr[From] =
      DdbUpdateExpr.PrependList(optic, items, elemSchema)
  }
}

object DdbExpr extends DdbExprSyntax {

  // ── ADT nodes ──────────────────────────────────────────────────────────────
  // Declared directly in this singleton object (not in DdbExprSyntax above) so they stay
  // path-independent — see the note on DdbExprSyntax for why.

  // Wraps a ZB SchemaExpr for scalar comparisons and sealed-trait equality (===, >, <, >=, <=, !=).
  // The interpreter delegates to fromDynamicSchemaExpr for these nodes; since v0.0.47
  // the embedded Literal carries Schema[_] so the codec is derived there for correct encoding.
  final case class Builtin[S](se: SchemaExpr[S, Boolean]) extends DdbExpr[S, Boolean]

  // DDB condition functions — carry the Optic directly.
  final case class AttributeExists[S, A](optic: Optic[S, A])    extends DdbExpr[S, Boolean]
  final case class AttributeNotExists[S, A](optic: Optic[S, A]) extends DdbExpr[S, Boolean]

  final case class Between[S, A](
    optic: Optic[S, A],
    lo: A,
    hi: A,
    schema: Schema[A]
  ) extends DdbExpr[S, Boolean]

  final case class In[S, A](
    optic: Optic[S, A],
    values: Seq[A],
    schema: Schema[A]
  ) extends DdbExpr[S, Boolean]

  // DynamoDB's `size(path)`: the length of a String or Binary, or the element count of a List, Map
  // or Set. Not a condition itself - comparing it with a number builds one.
  final case class Size[S, A](optic: Optic[S, A]) {
    def ===(n: Long): DdbExpr[S, Boolean]                = SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.Equal, n)
    def !=(n: Long): DdbExpr[S, Boolean]                 = SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.NotEqual, n)
    def <(n: Long): DdbExpr[S, Boolean]                  = SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.LessThan, n)
    def <=(n: Long): DdbExpr[S, Boolean]                 =
      SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.LessThanOrEqual, n)
    def >(n: Long): DdbExpr[S, Boolean]                  = SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.GreaterThan, n)
    def >=(n: Long): DdbExpr[S, Boolean]                 =
      SizeCompare(optic, DynamicSchemaExpr.RelationalOperator.GreaterThanOrEqual, n)
    def between(lo: Long, hi: Long): DdbExpr[S, Boolean] = SizeBetween(optic, lo, hi)
  }

  final case class SizeCompare[S, A](optic: Optic[S, A], op: DynamicSchemaExpr.RelationalOperator, value: Long)
      extends DdbExpr[S, Boolean]
  final case class SizeBetween[S, A](optic: Optic[S, A], lo: Long, hi: Long) extends DdbExpr[S, Boolean]

  final case class Contains[S](optic: Optic[S, String], value: String)    extends DdbExpr[S, Boolean]
  final case class BeginsWith[S](optic: Optic[S, String], prefix: String) extends DdbExpr[S, Boolean]

  // Checks whether a set attribute (NS/SS/BS) contains element B.
  // A is the set field type; B is the element type.
  final case class ContainsElement[S, A, B](
    optic: Optic[S, A],
    element: B,
    elemSchema: Schema[B]
  ) extends DdbExpr[S, Boolean]

  // Logical
  final case class And[S](left: DdbExpr[S, Boolean], right: DdbExpr[S, Boolean]) extends DdbExpr[S, Boolean]
  final case class Or[S](left: DdbExpr[S, Boolean], right: DdbExpr[S, Boolean])  extends DdbExpr[S, Boolean]
  final case class Not[S](inner: DdbExpr[S, Boolean])                            extends DdbExpr[S, Boolean]
}
