package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.DynamoDBError.ItemError
import zio.schema.Schema
import scala.collection.immutable.Set
import scala.util.Try

sealed trait AttributeValue { self =>
  type ScalaType

  def toAttrMap: Either[String, AttrMap] =
    self match {
      case AttributeValue.Map(map) =>
        Right(map.toList.map { case (avStr -> av) => avStr.value -> av }.foldLeft(AttrMap.empty)(_ + _))
      case x                       => Left(s"Expected a map, got: $x")
    }

  def decode[A](implicit schema: Schema[A]): Either[ItemError, A] = Codec.decoder(schema)(self)

  def ===[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From]                  = Equals(ValueOperand(self), that)
  def <>[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From]                   = NotEqual(ValueOperand(self), that)
  def <[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From]                    =
    LessThan(ValueOperand(self), that)
  def <=[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From]                   =
    LessThanOrEqual(ValueOperand(self), that)
  def >[From](that: Operand.Size[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    GreaterThanOrEqual(ValueOperand(self), that)

  def >=[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] =
    GreaterThanOrEqual(ValueOperand(self), that)

  def ===[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    Equals(ValueOperand(self), ProjectionExpressionOperand(that))
  def <>[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From]  =
    NotEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def <[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From]   =
    LessThan(ValueOperand(self), ProjectionExpressionOperand(that))
  def <=[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From]  =
    LessThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From]   =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
  def >=[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From]  =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))
}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  type WithScalaType[X] = AttributeValue { type ScalaType = X }

  private[dynamodb] final case class Binary(value: Iterable[Byte])              extends AttributeValue
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]]) extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                       extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])      extends AttributeValue { self =>
    def +(av: AttributeValue): List = List(self.value ++ Iterable(av))
  }
  private[dynamodb] object List {
    val empty = List(Iterable.empty)
  }

  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue { self =>
    def +(t: (ScalaString, AttributeValue)): Map = Map(self.value + (String(t._1) -> t._2))
  }
  private[dynamodb] object Map {
    val empty = Map(ScalaMap.empty)
  }

  private[dynamodb] final case class Number(value: BigDecimal)          extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])  extends AttributeValue { self =>
    def +(s: ScalaString): Either[ScalaString, NumberSet] =
      Try(BigDecimal(s)).toEither.left.map(_.getMessage).map(n => NumberSet(self.value + n))
  }
  private[dynamodb] final object NumberSet {
    val empty: NumberSet = NumberSet(Set.empty)
  }
  private[dynamodb] case object Null                                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)         extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString]) extends AttributeValue { self =>
    def +(s: ScalaString): StringSet = StringSet(self.value + s)
  }
  private[dynamodb] final object StringSet {
    val empty: StringSet = StringSet(Set.empty)
  }

  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue.WithScalaType[A] =
    ev.toAttributeValue(a).asInstanceOf[AttributeValue.WithScalaType[A]]

  def encode[A](a: A)(implicit schema: Schema[A]): AttributeValue = Codec.encoder(schema)(a)

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = scala.Predef.identity(_)
}
