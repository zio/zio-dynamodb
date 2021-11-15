package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.schema.Schema
import java.util.Base64

sealed trait AttributeValue { self =>
  def decode[A](implicit schema: Schema[A]): Either[String, A] = Decoder(schema)(self)

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

  // TODO(adam): Implement -- does this need to be an AliasMap???
  def render(): String =
    self match {
      case AttributeValue.Binary(value)    =>
        s""""B": "${Base64.getEncoder.encodeToString(value.toArray)}"""" // is base64 encoded when sent to AWS
      case AttributeValue.BinarySet(value) => value.toString()
      case AttributeValue.Bool(value)      =>
        value.toString
      case AttributeValue.List(value)      => value.map(_.render()).mkString("[", ",", "]")
      case AttributeValue.Map(value)       => value.toString()
      case AttributeValue.Number(value)    => value.toString()
      case AttributeValue.NumberSet(value) => value.toString()
      case AttributeValue.Null             => "null"
      case AttributeValue.String(value)    => value
      case AttributeValue.StringSet(value) => value.toString()
    }

}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  private[dynamodb] final case class Binary(value: Iterable[Byte])              extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]]) extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                       extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])      extends AttributeValue

  // TODO: use ListMap rather than Map
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue

  private[dynamodb] final case class Number(value: BigDecimal)          extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])  extends AttributeValue
  private[dynamodb] case object Null                                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)         extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString]) extends AttributeValue

  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue = ev.toAttributeValue(a)

  def encode[A](a: A)(implicit schema: Schema[A]): AttributeValue = Encoder(schema)(a)

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = identity(_)
}
