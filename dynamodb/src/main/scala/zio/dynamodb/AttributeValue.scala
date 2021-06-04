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

private[dynamodb] object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.{ Map => ScalaMap }

  private[dynamodb] final case class Binary(value: Iterable[Byte])                extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                         extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])        extends AttributeValue
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  private[dynamodb] final case class Number(value: BigDecimal)                    extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  private[dynamodb] object Null                                                   extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)                   extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString])           extends AttributeValue

  private[dynamodb] def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue = ev.toAttributeValue(a)
}

trait ToAttributeValueLowPriorityImplicits {
  implicit def listToAttributeValue[A](implicit element: ToAttributeValue[A]): ToAttributeValue[Iterable[A]] =
    (xs: Iterable[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))
}
