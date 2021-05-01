package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._

sealed trait AttributeValue { self =>

  // TODO: remove - maybe we could have this conversion as an implicit?
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

trait ToAttributeValue[-A] {
  def toAttributeValue(a: A): AttributeValue
}
object ToAttributeValue extends ToAttributeValueLowPriorityImplicits {
  import Predef.{ String => ScalaString }
  import Predef.{ Map => ScalaMap }

  implicit val binaryToAttributeValue: ToAttributeValue[Chunk[Byte]]           = AttributeValue.Binary(_)
  implicit val binarySetToAttributeValue: ToAttributeValue[Chunk[Chunk[Byte]]] = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean]                 = AttributeValue.Bool(_)

  implicit def mapToAttributeValue[A](implicit
    element: ToAttributeValue[A]
  ): ToAttributeValue[ScalaMap[ScalaString, A]] =
    (map: ScalaMap[ScalaString, A]) =>
      AttributeValue.Map {
        map.map {
          case (key, value) => (AttributeValue.String(key), element.toAttributeValue(value))
        }
      }

  implicit val stringToAttributeValue: ToAttributeValue[String]              = AttributeValue.String(_)
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]] =
    AttributeValue.StringSet(_)
  implicit val numberToAttributeValue: ToAttributeValue[BigDecimal]          = AttributeValue.Number(_)
  implicit val numberSetToAttributeValue: ToAttributeValue[Set[BigDecimal]]  = AttributeValue.NumberSet(_)
}

trait ToAttributeValueLowPriorityImplicits {
  implicit def listToAttributeValue[A](implicit element: ToAttributeValue[A]): ToAttributeValue[Iterable[A]] =
    (xs: Iterable[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))
}

/*
TODO: implicit conversions: (may help)
AV => Operand
PE => Operand
 */
