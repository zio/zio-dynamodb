package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression.{
  Between,
  Equals,
  GreaterThanOrEqual,
  In,
  LessThan,
  LessThanOrEqual,
  NotEqual,
  Operand
}

sealed trait AttributeValue { self =>

  // TODO: remove
  def operand: ConditionExpression.Operand = ConditionExpression.Operand.ValueOperand(self)

  def between(minValue: AttributeValue, maxValue: AttributeValue): ConditionExpression =
    Between(ValueOperand(self), minValue, maxValue)
  def in(values: Set[AttributeValue]): ConditionExpression                             = In(ValueOperand(self), values)

  def ===(that: AttributeValue): ConditionExpression = Equals(ValueOperand(self), ValueOperand(that))

  def <>(that: AttributeValue): ConditionExpression = NotEqual(ValueOperand(self), ValueOperand(that))
  def <(that: AttributeValue): ConditionExpression  = LessThan(ValueOperand(self), ValueOperand(that))
  def <=(that: AttributeValue): ConditionExpression = LessThanOrEqual(ValueOperand(self), ValueOperand(that))
  def >(that: AttributeValue): ConditionExpression  = GreaterThanOrEqual(ValueOperand(self), ValueOperand(that))
  def >=(that: AttributeValue): ConditionExpression = GreaterThanOrEqual(ValueOperand(self), ValueOperand(that))

  def ===(that: Operand.Size): ConditionExpression = Equals(ValueOperand(self), that)
  def <>(that: Operand.Size): ConditionExpression  = NotEqual(ValueOperand(self), that)
  def <(that: Operand.Size): ConditionExpression   = LessThan(ValueOperand(self), that)
  def <=(that: Operand.Size): ConditionExpression  = LessThanOrEqual(ValueOperand(self), that)
  def >(that: Operand.Size): ConditionExpression   = GreaterThanOrEqual(ValueOperand(self), that)
  def >=(that: Operand.Size): ConditionExpression  = GreaterThanOrEqual(ValueOperand(self), that)

  /*
  x:ProjectionExpression = ???
  v: AttributeValue = ???
  v == x.size
   */

}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.{ Map => ScalaMap }

  final case class Binary(value: Chunk[Byte])                   extends AttributeValue
  final case class Bool(value: Boolean)                         extends AttributeValue
  final case class BinarySet(value: Chunk[Chunk[Byte]])         extends AttributeValue
  final case class List(value: Chunk[AttributeValue])           extends AttributeValue
  final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  final case class Number(value: BigDecimal)                    extends AttributeValue
  final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  object Null                                                   extends AttributeValue
  final case class String(value: ScalaString)                   extends AttributeValue
  final case class StringSet(value: Set[ScalaString])           extends AttributeValue
}

/*
TODO: implicit conversions: (may help)
AV => Operand
PE => Operand
 */
