package zio.dynamodb

import zio.Chunk
import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._

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

trait ToAttributeValue[-A] {
  def toAttributeValue(a: A): AttributeValue
}

/*
  final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  object Null                                                   extends AttributeValue
  DONE
  final case class List(value: Chunk[AttributeValue])           extends AttributeValue
  final case class Binary(value: Chunk[Byte])                   extends AttributeValue
  final case class BinarySet(value: Chunk[Chunk[Byte]])         extends AttributeValue
  final case class Bool(value: Boolean)                         extends AttributeValue
  final case class Number(value: BigDecimal)                    extends AttributeValue
  final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  final case class String(value: ScalaString)                   extends AttributeValue
  final case class StringSet(value: Set[ScalaString])           extends AttributeValue
 */

object ToAttributeValue {
  import Predef.{ String => ScalaString }

//  implicit def mapToAttributeValue[A](implicit element: ToAttributeValue[A]): ToAttributeValue[Chunk[A]] =
//    new ToAttributeValue[ScalaMap[String, A]] {
//      override def toAttributeValue(map: ScalaMap[String, A]): AttributeValue =
//        AttributeValue.Map {
//          val x: ScalaMap[ScalaString, AttributeValue] = map.map {
//            case (key, value) => (key, element.toAttributeValue(value))
//          }
//          x
//        }
//    }

  implicit val binaryToAttributeValue: ToAttributeValue[Chunk[Byte]]           = AttributeValue.Binary(_)
  implicit val binarySetToAttributeValue: ToAttributeValue[Chunk[Chunk[Byte]]] = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean]                 = AttributeValue.Bool(_)
  implicit val boolSetToAttributeValue: ToAttributeValue[Boolean]              = AttributeValue.Bool(_)

  /*
  TODO: try to make this more general ie Iterable rather than Chunk
  However when I do this I get a clash with StringSet which implements iterable
    ambiguous implicit values:
     both method listToAttributeValue in object ToAttributeValue of type [A](implicit element: zio.dynamodb.ToAttributeValue[A]): zio.dynamodb.ToAttributeValue[Iterable[A]]
     and value stringSetToAttributeValue in object ToAttributeValue of type zio.dynamodb.ToAttributeValue[Set[String]]
     match expected type zio.dynamodb.ToAttributeValue[scala.collection.immutable.Set[String]]
      val pe2        = path1.set(Set("s"))
   */
  implicit def listToAttributeValue[A](implicit element: ToAttributeValue[A]): ToAttributeValue[Chunk[A]] =
    new ToAttributeValue[Chunk[A]] { // TODO: convert to single abstract method
      override def toAttributeValue(xs: Chunk[A]): AttributeValue =
        AttributeValue.List(xs.map(element.toAttributeValue))
    }

  implicit val stringToAttributeValue: ToAttributeValue[String]              = AttributeValue.String(_) // single abstract method
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]] =
    AttributeValue.StringSet(_)
  implicit val numberToAttributeValue: ToAttributeValue[BigDecimal]          = AttributeValue.Number(_)
  implicit val numberSetToAttributeValue: ToAttributeValue[Set[BigDecimal]]  = AttributeValue.NumberSet(_)
}

/*
TODO: implicit conversions: (may help)
AV => Operand
PE => Operand
 */
