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

import zio.blocks.schema.{ Optic, Schema, SchemaExpr }
import zio.blocks.schema.comptime.Allows
import Allows.Wrapped
import zio.dynamodb.blocks.DdbGrammar
import zio.dynamodb.blocks.OpticToPE
import zio.dynamodb.blocks.schema.DynamoDBCodec
import zio.dynamodb.compat.||
import zio.dynamodb.{ AttributeValue, ProjectionExpression }
import zio.dynamodb.UpdateExpression.RenderableAction
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, Failure, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand

import scala.annotation.unused
import scala.language.implicitConversions

/**
 * Typed DDB condition expression ADT.
 *
 *  Scalar comparisons and sealed-trait equality use ZB [[zio.blocks.schema.Optic]]
 *  operators directly ([[zio.blocks.schema.Optic.===]], `>`, `<`, etc.), which produce
 *  [[SchemaExpr]][S, Boolean]. The [[DdbExprSyntax.schemaExprToDdbExpr]] implicit lifts them
 *  into [[DdbExpr.Builtin]]. Since zio-blocks v0.0.47
 *  [[zio.blocks.schema.DynamicSchemaExpr.Literal]] carries a [[Schema]], the interpreter
 *  derives a [[zio.dynamodb.blocks.schema.DynamoDBCodec]] at evaluation time and encodes
 *  sealed-trait values correctly — no special workaround needed.
 *
 *  DDB-specific functions ([[DdbExprSyntax.OpticDdbExprOps.attributeExists]],
 *  [[DdbExprSyntax.OpticDdbExprOps.between]], etc.) are extension methods on
 *  [[Optic]] and produce [[DdbExpr]] nodes directly.
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
trait DdbExprSyntax extends DerivedCodecSyntax {

  // ── Optic extension methods ─────────────────────────────────────────────────

  // DDB-specific ops for any Optic[S, A]. ZB's Optic already provides ===, >,
  // <, >= etc. as direct methods returning SchemaExpr; we add DDB functions here.
  // Not `extends AnyVal`: value classes may only be top-level or object members, not
  // trait members, and this trait is mixed into more than one object (DdbExpr, dsl).
  implicit class OpticDdbExprOps[S, A](private val optic: Optic[S, A]) {
    def attributeExists: DdbExpr[S, Boolean]                                         = DdbExpr.AttributeExists(optic)
    def attributeNotExists: DdbExpr[S, Boolean]                                      =
      DdbExpr.AttributeNotExists(optic)
    def between(lo: A, hi: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.N || DdbGrammar.S || DdbGrammar.B]
    ): DdbExpr[S, Boolean]                                                           =
      DdbExpr.Between(optic, lo, hi, codec)
    def in(head: A, rest: A*)(implicit codec: DynamoDBCodec[A]): DdbExpr[S, Boolean] =
      DdbExpr.In(optic, head +: rest, codec)
    def inSet(values: Set[A])(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.N || DdbGrammar.S || DdbGrammar.B]
    ): DdbExpr[S, Boolean]                                                           =
      DdbExpr.In(optic, values.toSeq, codec)
    def containsElement[B](element: B)(implicit
      elemCodec: DynamoDBCodec[B],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): DdbExpr[S, Boolean]                                                           =
      DdbExpr.ContainsElement(optic, element, elemCodec)
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
  // &&/||: Since ZB v0.0.47+ removes &&/|| as direct methods on SchemaExpr (moving them
  // to the BooleanOps companion implicit class), Scala 2 no longer suppresses this bridge
  // for DdbExpr RHS. SchemaExpr && DdbExpr now resolves here instead of failing.
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

  // Extension methods on Optic[From, A] that produce UpdateExpression.Action nodes.
  // Literals are encoded via DynamoDBCodec[A] — same codec-carrying principle as
  // the condition expression ops above, so sealed-trait encoding rules are respected.
  //
  // Allows[A, ...] constraints mirror schema-expr's LensUpdateExprSyntax — they are
  // orthogonal to codec encoding and prevent operations from being applied to
  // fields of incompatible DynamoDB types at compile time.
  //
  // Import: `import DdbExpr._` (same import as condition expression ops).
  implicit class OpticUpdateOps[From, A](private val optic: Optic[From, A]) {

    // OpticToPE.pe can fail for optic shapes DDB paths can't represent (eg a Map
    // key that isn't a String) — deferred as Action.Failure rather than thrown,
    // so it is collectable/reported the same way as ConditionExpression failures.
    private def pe: Either[String, ProjectionExpression[From, A]] =
      OpticToPE.pe(optic)

    // SET path = value   (valid for any attribute type)
    def set(value: A)(implicit codec: DynamoDBCodec[A]): RenderableAction[From] =
      pe.fold(Failure(_), p => SetAction(p, SetOperand.ValueOperand(codec.encoder(value))))

    // SET path = other_path   (copy one attribute to another)
    def set(other: Optic[From, A]): RenderableAction[From] =
      (pe, OpticToPE.pe(other)) match {
        case (Right(p), Right(otherPE)) => SetAction(p, SetOperand.PathOperand(otherPE))
        case (Left(msg), _)             => Failure(msg)
        case (_, Left(msg))             => Failure(msg)
      }

    // SET path = if_not_exists(path, value)   (set only when attribute is absent)
    def setIfNotExists(value: A)(implicit codec: DynamoDBCodec[A]): RenderableAction[From] =
      pe.fold(Failure(_), p => SetAction(p, SetOperand.IfNotExists(p, codec.encoder(value))))

    // REMOVE path   (valid for any attribute type)
    def remove: RenderableAction[From] = pe.fold(Failure(_), RemoveAction(_))

    // REMOVE path[index]   (list fields only)
    def remove(index: Int)(implicit @unused ev: Allows[A, DdbGrammar.L]): RenderableAction[From] =
      pe.fold(Failure(_), p => RemoveAction(ProjectionExpression.ListElement(p, index)))

    // SET path = path + delta   (atomic in-place increment; numeric fields only)
    def increment(delta: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): RenderableAction[From] =
      pe.fold(
        Failure(_),
        p => SetAction(p, SetOperand.PathOperand(p) + SetOperand.ValueOperand[A](codec.encoder(delta)))
      )

    // SET path = path - delta   (atomic in-place decrement; numeric fields only)
    def decrement(delta: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): RenderableAction[From] =
      pe.fold(
        Failure(_),
        p => SetAction(p, SetOperand.PathOperand(p) - SetOperand.ValueOperand[A](codec.encoder(delta)))
      )

    // ADD path value   (numeric fields only; for set union see addSet)
    def add(value: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.N || Wrapped[DdbGrammar.N]]
    ): RenderableAction[From] =
      pe.fold(Failure(_), p => AddAction(p, codec.encoder(value)))

    // ADD path set   (set attributes: NS, SS, or BS — union of sets)
    def addSet(value: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): RenderableAction[From] =
      pe.fold(Failure(_), p => AddAction(p, codec.encoder(value)))

    // DELETE path set   (set attributes: NS, SS, or BS — remove elements from a set)
    def deleteFromSet(value: A)(implicit
      codec: DynamoDBCodec[A],
      @unused ev: Allows[A, DdbGrammar.NS || DdbGrammar.SS || DdbGrammar.BS]
    ): RenderableAction[From] =
      pe.fold(Failure(_), p => DeleteAction(p, codec.encoder(value)))

    // SET path = list_append(path, [...items])   (list fields only)
    def appendList[B](items: Seq[B])(implicit
      elemCodec: DynamoDBCodec[B],
      @unused ev: Allows[A, DdbGrammar.L]
    ): RenderableAction[From] =
      pe.fold(
        Failure(_),
        p => SetAction(p, SetOperand.ListAppend(p, AttributeValue.List(items.map(elemCodec.encoder).toList)))
      )

    // SET path = list_append([...items], path)   (list fields only)
    def prependList[B](items: Seq[B])(implicit
      elemCodec: DynamoDBCodec[B],
      @unused ev: Allows[A, DdbGrammar.L]
    ): RenderableAction[From] =
      pe.fold(
        Failure(_),
        p => SetAction(p, SetOperand.ListPrepend(p, AttributeValue.List(items.map(elemCodec.encoder).toList)))
      )
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
    codec: DynamoDBCodec[A]
  ) extends DdbExpr[S, Boolean]

  final case class In[S, A](
    optic: Optic[S, A],
    values: Seq[A],
    codec: DynamoDBCodec[A]
  ) extends DdbExpr[S, Boolean]

  final case class Contains[S](optic: Optic[S, String], value: String)    extends DdbExpr[S, Boolean]
  final case class BeginsWith[S](optic: Optic[S, String], prefix: String) extends DdbExpr[S, Boolean]

  // Checks whether a set attribute (NS/SS/BS) contains element B.
  // A is the set field type; B is the element type.
  final case class ContainsElement[S, A, B](
    optic: Optic[S, A],
    element: B,
    elemCodec: DynamoDBCodec[B]
  ) extends DdbExpr[S, Boolean]

  // Logical
  final case class And[S](left: DdbExpr[S, Boolean], right: DdbExpr[S, Boolean]) extends DdbExpr[S, Boolean]
  final case class Or[S](left: DdbExpr[S, Boolean], right: DdbExpr[S, Boolean])  extends DdbExpr[S, Boolean]
  final case class Not[S](inner: DdbExpr[S, Boolean])                            extends DdbExpr[S, Boolean]
}
