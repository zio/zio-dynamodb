package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._

sealed trait AttributeValue { self =>

  def ===(that: Operand.Size): ConditionExpression = Equals(ValueOperand(self), that)
  def <>(that: Operand.Size): ConditionExpression  = NotEqual(ValueOperand(self), that)
  def <(that: Operand.Size): ConditionExpression   = LessThan(ValueOperand(self), that)
  def <=(that: Operand.Size): ConditionExpression  = LessThanOrEqual(ValueOperand(self), that)
  def >(that: Operand.Size): ConditionExpression   = GreaterThanOrEqual(ValueOperand(self), that)
  def >=(that: Operand.Size): ConditionExpression  = GreaterThanOrEqual(ValueOperand(self), that)

  def ===(that: ProjectionExpression): ConditionExpression =
    Equals(ValueOperand(self), ProjectionExpressionOperand(that))
  def <>(that: ProjectionExpression): ConditionExpression  =
    NotEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def <(that: ProjectionExpression): ConditionExpression   =
    LessThan(ValueOperand(self), ProjectionExpressionOperand(that))
  def <=(that: ProjectionExpression): ConditionExpression  =
    LessThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >(that: ProjectionExpression): ConditionExpression   =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >=(that: ProjectionExpression): ConditionExpression  =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))

}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.{ Map => ScalaMap }

  final case class Binary(value: Iterable[Byte])                extends AttributeValue
  final case class Bool(value: Boolean)                         extends AttributeValue
  final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  final case class List(value: Iterable[AttributeValue])        extends AttributeValue
  final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  final case class Number(value: BigDecimal)                    extends AttributeValue
  final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  object Null                                                   extends AttributeValue
  final case class String(value: ScalaString)                   extends AttributeValue
  final case class StringSet(value: Set[ScalaString])           extends AttributeValue

  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue = ev.toAttributeValue(a)
}

trait ToAttributeValueLowPriorityImplicits {
  implicit def listToAttributeValue[A](implicit element: ToAttributeValue[A]): ToAttributeValue[Iterable[A]] =
    (xs: Iterable[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))
}
