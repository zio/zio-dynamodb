package zio.dynamodb

import zio.dynamodb.UpdateExpression.SetOperand.{ IfNotExists, ListAppend, ListPrepend, PathOperand }
import zio.dynamodb.proofs.{ Addable, Containable }

private[dynamodb] object ProjectionExpressionOps {

  def set[From, To: ToAttributeValue](
    self: ProjectionExpression[From, To],
    a: To
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(
      self,
      UpdateExpression.SetOperand.ValueOperand(
        ToAttributeValue[To].toAttributeValue(a)
      )
    )

  def setExpr[From, To](
    self: ProjectionExpression[From, To],
    pe: ProjectionExpression[From, To]
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(self, PathOperand(pe))

  def setIfNotExists[From, To: ToAttributeValue](
    self: ProjectionExpression[From, To],
    a: To
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(
      self,
      IfNotExists(self, ToAttributeValue[To].toAttributeValue(a))
    )

  def append[From, To, A](
    self: ProjectionExpression[From, To],
    a: A
  )(implicit
    ev: To <:< Iterable[A],
    to: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction[From, To] =
    appendList(self, List(a).asInstanceOf[To])

  def appendList[From, To, A](
    self: ProjectionExpression[From, To],
    xs: To
  )(implicit
    ev: To <:< Iterable[A],
    to: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(
      self,
      ListAppend(
        self,
        AttributeValue.List(xs.toList.map(to.toAttributeValue))
      )
    )

  def prepend[From, To, A](
    self: ProjectionExpression[From, To],
    a: A
  )(implicit
    ev: To <:< Iterable[A],
    to: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(
      self,
      ListPrepend(
        self,
        AttributeValue.List(List(a).map(to.toAttributeValue))
      )
    )

  def prependList[From, To, A](
    self: ProjectionExpression[From, To],
    xs: To
  )(implicit
    ev: To <:< Iterable[A],
    to: ToAttributeValue[A]
  ): UpdateExpression.Action.SetAction[From, To] =
    UpdateExpression.Action.SetAction(
      self,
      ListPrepend(
        self,
        AttributeValue.List(xs.toList.map(to.toAttributeValue))
      )
    )

  def between[From, To: ToAttributeValue](
    self: ProjectionExpression[From, To],
    minValue: To,
    maxValue: To
  ): ConditionExpression[From] =
    ConditionExpression.Operand
      .ProjectionExpressionOperand(self)
      .between(
        ToAttributeValue[To].toAttributeValue(minValue),
        ToAttributeValue[To].toAttributeValue(maxValue)
      )

  def deleteFromSet[From, To](
    self: ProjectionExpression[From, To],
    set: To
  )(implicit
    ev: To <:< Set[_],
    to: ToAttributeValue[To]
  ): UpdateExpression.Action.DeleteAction[From] =
    UpdateExpression.Action.DeleteAction(
      self,
      to.toAttributeValue(set)
    )

  def inSet[From, To: ToAttributeValue](
    self: ProjectionExpression[From, To],
    values: Set[To]
  ): ConditionExpression[From] =
    ConditionExpression.Operand
      .ProjectionExpressionOperand(self)
      .in(values.map(ToAttributeValue[To].toAttributeValue))

  def inValues[From, To: ToAttributeValue](
    self: ProjectionExpression[From, To],
    value: To,
    values: To*
  ): ConditionExpression[From] = {
    val set = values.toSet + value
    ConditionExpression.Operand
      .ProjectionExpressionOperand(self)
      .in(set.map(ToAttributeValue[To].toAttributeValue))
  }

  def contains[From, To, A](
    self: ProjectionExpression[From, To],
    a: A
  )(implicit
    ev: Containable[To, A],
    to: ToAttributeValue[A]
  ): ConditionExpression[From] =
    ConditionExpression.Contains(self, to.toAttributeValue(a))

  def containsSet[From, To, A](
    self: ProjectionExpression[From, To],
    head: A,
    tail: Set[A]
  )(implicit
    ev: Containable[To, A],
    to: ToAttributeValue[A]
  ): ConditionExpression[From] =
    tail.foldLeft(contains(self, head))((acc, a) => acc && contains(self, a))

  def add[From, To](
    self: ProjectionExpression[From, To],
    a: To
  )(implicit
    ev: Addable[To, To],
    to: ToAttributeValue[To]
  ): UpdateExpression.Action.AddAction[From] =
    UpdateExpression.Action.AddAction(
      self,
      to.toAttributeValue(a)
    )

  def addSet[From, To, A](
    self: ProjectionExpression[From, To],
    set: Set[A]
  )(implicit
    ev: Addable[To, A],
    evSet: Set[A] <:< To,
    to: ToAttributeValue[To]
  ): UpdateExpression.Action.AddAction[From] =
    UpdateExpression.Action.AddAction(
      self,
      to.toAttributeValue(evSet(set))
    )
}
