package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.DynamoDBError.ItemError
import scala.collection.immutable.Set
import scala.util.Try

sealed trait AttributeValue { self =>
  type ScalaType

  def toAttrMap: Either[ItemError, AttrMap] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(self)

  def decode[A: SchemaCodec]: Either[ItemError, A] = SchemaCodec[A].decoder(self)

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

  private[dynamodb] final val showType: String =
    self match {
      case _: AttributeValue.Binary    => "AttributeValue.Binary"
      case _: AttributeValue.BinarySet => "AttributeValue.BinarySet"
      case _: AttributeValue.Bool      => "AttributeValue.Bool"
      case _: AttributeValue.List      => "AttributeValue.List"
      case _: AttributeValue.Map       => "AttributeValue.Map"
      case _: AttributeValue.Number    => "AttributeValue.Number"
      case _: AttributeValue.NumberSet => "AttributeValue.NumberSet"
      case _: AttributeValue.Null.type => "AttributeValue.Null"
      case _: AttributeValue.String    => "AttributeValue.String"
      case _: AttributeValue.StringSet => "AttributeValue.StringSet"
    }
}

object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  def single(fieldName: ScalaString, av: AttributeValue): Map = Map(ScalaMap(String(fieldName) -> av))

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

    // For small insertions with a small no of items its OK to use immutable Map vs MapBuilder as later still does a
    // copy at the end
    def +(t: (ScalaString, AttributeValue)): Map = {
      val (s, av) = t
      Map(self.value + ((String(s), av)))
    }

    def get(key: ScalaString): Option[AttributeValue] = {
      val (s, av) = (key, self.value) // TODO: extract
      av.get(String(s))
    }

    def size: Int = self.value.size
  }

  private[dynamodb] object Map {
    val empty = new Map(ScalaMap.empty)

    // TODO: find occurrences of "AttributeValue.Map(Map" or "AttributeValue.Map(ScalaMap"
    def apply(fieldName: ScalaString, value: AttributeValue): Map = {
      val (s, av) = (fieldName, value)
      Map(ScalaMap((String(s), av)))
    }
  }

  private[dynamodb] final case class Number(value: BigDecimal)          extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])  extends AttributeValue { self =>
    def +(s: ScalaString): Either[ScalaString, NumberSet] =
      Try(BigDecimal(s)).toEither.left.map(_.getMessage).map(n => NumberSet(self.value + n))
  }
  private[dynamodb] object NumberSet {
    val empty: NumberSet = NumberSet(Set.empty)
  }
  private[dynamodb] case object Null                                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)         extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString]) extends AttributeValue { self =>
    def +(s: ScalaString): StringSet = StringSet(self.value + s)
  }
  private[dynamodb] object StringSet {
    val empty: StringSet = StringSet(Set.empty)
  }

  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue.WithScalaType[A] =
    ev.toAttributeValue(a).asInstanceOf[AttributeValue.WithScalaType[A]]

  def encode[A: SchemaCodec](a: A): AttributeValue = SchemaCodec[A].encoder(a)

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = scala.Predef.identity(_)
}
