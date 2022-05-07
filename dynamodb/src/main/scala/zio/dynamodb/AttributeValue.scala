package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.schema.Schema

sealed trait AttributeValue { self =>
  def decode[A](implicit schema: Schema[A]): Either[String, A] = Codec.decoder(schema)(self)

  def ===(that: Operand.Size): ConditionExpression = Equals(ValueOperand(self), that)
  def <>(that: Operand.Size): ConditionExpression  = NotEqual(ValueOperand(self), that)
  def <(that: Operand.Size): ConditionExpression   = LessThan(ValueOperand(self), that)
  def <=(that: Operand.Size): ConditionExpression  = LessThanOrEqual(ValueOperand(self), that)
  def >(that: Operand.Size): ConditionExpression   = GreaterThanOrEqual(ValueOperand(self), that)
  def >=(that: Operand.Size): ConditionExpression  = GreaterThanOrEqual(ValueOperand(self), that)

  def ===(that: ProjectionExpression[_]): ConditionExpression =
    Equals(ValueOperand(self), ProjectionExpressionOperand(that))
  def <>(that: ProjectionExpression[_]): ConditionExpression  =
    NotEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def <(that: ProjectionExpression[_]): ConditionExpression   =
    LessThan(ValueOperand(self), ProjectionExpressionOperand(that))
  def <=(that: ProjectionExpression[_]): ConditionExpression  =
    LessThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >(that: ProjectionExpression[_]): ConditionExpression   =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >=(that: ProjectionExpression[_]): ConditionExpression  =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  private[dynamodb] final case class Binary(value: Iterable[Byte])              extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]]) extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                       extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])      extends AttributeValue

  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue

  private[dynamodb] final case class Number(value: BigDecimal)          extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])  extends AttributeValue
  private[dynamodb] case object Null                                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)         extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString]) extends AttributeValue

  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue = ev.toAttributeValue(a)

  def encode[A](a: A)(implicit schema: Schema[A]): AttributeValue = Codec.encoder(schema)(a)

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = identity(_)
}
