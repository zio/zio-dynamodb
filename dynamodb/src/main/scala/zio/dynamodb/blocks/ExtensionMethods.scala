package zio.dynamodb.blocks

import zio.blocks.schema.Optic
import zio.dynamodb.{ AttributeValue, ConditionExpression, ProjectionExpression, ToAttributeValue, UpdateExpression }
import zio.dynamodb.UpdateExpression.SetOperand.{ ListAppend, ListPrepend, PathOperand }

object ExtensionMethods {
  import zio.dynamodb.blocks.compat.Or
  import zio.blocks.schema.comptime.Allows
  import Allows._
  import zio.blocks.typeid.IsNominalType

  // scalars
  type N    =
    Primitive.Int Or Primitive.Long Or Primitive.Float Or Primitive.Double Or Primitive.Short Or Wrapped[Self]
  type S    = Primitive.String Or Wrapped[Self]
  type BOOL = Primitive.Boolean
  type B    = Sequence[Primitive.Byte] Or Wrapped[Self]
  // I think we can ignore NULL for incoming Scala types

  type NS = Sequence.Set[N Or Wrapped[N]]
  type SS = Sequence.Set[S Or Wrapped[S]]
  type BS = Sequence.Set[B]

  // list excludes Sets - note we need to explicitly add Record here for List[Address]
  type L = Sequence.List[All Or Record[All]] Or Sequence.Vector[All Or Record[All]] Or Sequence.Array[
    All Or Record[All]
  ] Or
    Sequence.Chunk[All | Record[All]]

  type M = Map[Primitive.String, All]

  // single recursive root
  type All =
    N Or S Or BOOL Or B Or NS Or SS Or BS Or Record[Self] Or Sequence[Self] Or Map[Self, Self]

  implicit class OpticToDdbExpr[From, To: ToAttributeValue](optic: Optic[From, To]) {
    private def self: ProjectionExpression[From, To] = OpticToPE.pe(optic)

    /*
ADD update behaviour
| Attribute Type    | Allowed? | Behaviour         |
| ----------------- | -------- | ----------------- |
| `N` (Number)      | ✅        | Numeric increment |
| `NS` (Number Set) | ✅        | Set union         |
| `SS` (String Set) | ✅        | Set union         |
| `BS` (Binary Set) | ✅        | Set union         |
| `S` (String)      | ❌        | Not allowed       |
| `L` (List)        | ❌        | Not allowed       |
| `M` (Map)         | ❌        | Not allowed       |
| `BOOL`            | ❌        | Not allowed       |
| `NULL`            | ❌        | Not allowed       |
     */

    def add[A](a: A)(implicit
      ev: Allows[A, N Or Wrapped[N]],
      ev2: Allows[To, N Or Wrapped[N]],
      to: ToAttributeValue[A]
    ): UpdateExpression.Action.AddAction[From] = {
      val (_, _) = (ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.AddAction(
        self,
        to.toAttributeValue(a)
      )
    }

    /** Only applies to a List */
    def appendList[A](
      xs: To
    )(implicit
      ev: Allows[To, L],
      //        ev2: Allows[To, Sequence[IsType[A]]],
      ev3: To <:< Iterable[A],
      to: ToAttributeValue[A]
    ): UpdateExpression.Action.SetAction[From, To] = {
      val (_, _) = (ev, ev3) // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.SetAction(
        self,
        ListAppend(
          self,
          AttributeValue.List(xs.toList.map(to.toAttributeValue))
        )
      )
    }

    def addSet[A](
      set: Set[A]
    )(implicit
      ev: Allows[To, NS Or SS Or BS],
      evSet: Set[A] <:< To
    ): UpdateExpression.Action.AddAction[From] = {
      val _ = ev // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.AddAction(
        self,
        ToAttributeValue[To].toAttributeValue(evSet(set))
      )
    }

    /** valid for N | S | B */
    def between(
      minValue: To,
      maxValue: To
    )(implicit ex: Allows[To, N Or S Or B]): ConditionExpression[From] = {
      val _ = ex // to silence unused warnings - we just need the evidence for the compiler
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .between(
          ToAttributeValue[To].toAttributeValue(minValue),
          ToAttributeValue[To].toAttributeValue(maxValue)
        )
    }

    def contains[A](a: A)(implicit
      ev0: IsNominalType[A],
      ev: Allows[To, NS Or SS Or BS Or L],
      ev2: Allows[To, Sequence[IsType[A]]],
      to: ToAttributeValue[A]
    ): ConditionExpression[From] = {

      val (_, _, _) = (ev0, ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
      ConditionExpression.Contains(self, to.toAttributeValue(a))
    }

    def contains(a: String)(implicit
      ev: Allows[To, S]
    ): ConditionExpression[From] = {
      val _ = ev // to silence unused warnings - we just need the evidence for the compiler
      ConditionExpression.Contains(self, AttributeValue.String(a))
    }

    def deleteFromSet(
      set: To
    )(implicit
      ev: Allows[To, NS Or SS Or BS],
      to: ToAttributeValue[To]
    ): UpdateExpression.Action.DeleteAction[From] = {
      val _ = ev // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.DeleteAction(
        self,
        to.toAttributeValue(set)
      )
    }

    def prependList[A](
      xs: To
    )(implicit
      ev: Allows[To, L],
      ev2: To <:< Iterable[A],
      to: ToAttributeValue[A]
    ): UpdateExpression.Action.SetAction[From, To] = {
      val (_, _) = (ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.SetAction(
        self,
        ListPrepend(
          self,
          AttributeValue.List(xs.toList.map(to.toAttributeValue))
        )
      )
    }

    /** Attribute must be a scalar ie N | S | B */
    def inSet(
      values: Set[To]
    )(implicit ev: Allows[To, N Or S Or B]): ConditionExpression[From] = {
      val _ = ev // to silence unused warnings - we just need the evidence for the compiler
      ConditionExpression.Operand
        .ProjectionExpressionOperand(self)
        .in(values.map(ToAttributeValue[To].toAttributeValue))
    }

    // TODO: prepend - only valid for a L attribute

    /**
     * Removes this PathExpression from an item - always valid as we have a valid path via an optic in hand
     */
    def remove: UpdateExpression.Action.RemoveAction[From] =
      UpdateExpression.Action.RemoveAction[From](self)

    /*
Remove at index UpdateExpression behaviour
| Attribute Type | Allowed? |
| -------------- | -------- |
| `L` (List)     | ✅       |
| `SS`           | ❌       |
| `NS`           | ❌        |
| `BS`           | ❌        |
| `N`            | ❌        |
| `S`            | ❌        |
| `M`            | ❌        |
     */
    def remove(
      index: Int // we need extra constraint to exclude Sets etc: evSeq: To <:< Seq[_]
    )(implicit ev: Allows[To, L]): UpdateExpression.Action.RemoveAction[From] = {
      val _ = ev // to silence unused warnings - we just need the evidence for the compiler
      UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
    }

    def set(
      a: To
    ): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        self,
        UpdateExpression.SetOperand.ValueOperand(
          ToAttributeValue[To].toAttributeValue(a)
        )
      )

    def set(
      o: Optic[From, To]
    ): UpdateExpression.Action.SetAction[From, To] = {
      val oAsPE = OpticToPE.pe(o)
      UpdateExpression.Action.SetAction(self, PathOperand(oAsPE))
    }

  }

}
