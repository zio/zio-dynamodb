package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.DynamoDBError.ItemError
import zio.schema.Schema
import scala.collection.immutable.Set
import scala.util.Try

sealed trait AttributeValue { self =>
  type ScalaType

  def toAttrMap: Either[ItemError, AttrMap] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(self)

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

  private[dynamodb] final case class Map(
    // TODO: Avi - replace with JMapView
    private val value: ScalaMap[String, AttributeValue]
  ) extends AttributeValue {
    self =>

    // TODO: Avi - replace with .addOne
    def +(t: (ScalaString, AttributeValue)): Map = {
      val (s, av) = t
      Map(value + ((String(s), av)))
    }

    def +(m: Map): Map = Map(self.value ++ m.value)

    def size: Int = self.value.size
  }

  private[dynamodb] object Map {
    object hash   {
      def builder: Builder =
        new Builder(
          new java.util.HashMap[String, AttributeValue](),
          m => new java.util.HashMap[String, AttributeValue](m)
        )

      def single(key: String, value: AttributeValue): AttributeValue.Map = {
        val map = new java.util.HashMap[String, AttributeValue](1)
        map.put(key, value)
        AttributeValue.Map(
          new JMapView(map, m => new java.util.HashMap[String, AttributeValue](m))
        )
      }

      val empty: AttributeValue.Map = AttributeValue.Map(JMapView.emptyHashMap)

    }
    object linked {
      def builder: Builder =
        new Builder(
          new java.util.LinkedHashMap[String, AttributeValue](),
          m => new java.util.LinkedHashMap[String, AttributeValue](m)
        )

      def single(key: String, value: AttributeValue): AttributeValue.Map = {
        val map = new java.util.LinkedHashMap[String, AttributeValue](1)
        map.put(key, value)
        AttributeValue.Map(
          new JMapView(map, m => new java.util.LinkedHashMap[String, AttributeValue](m))
        )
      }

      val empty: AttributeValue.Map = AttributeValue.Map(JMapView.emptyLinkedHashMap)

    }

    final class Builder(
      private val underlying: java.util.Map[String, AttributeValue],
      private val cloneFn: java.util.Map[String, AttributeValue] => java.util.Map[
        String,
        AttributeValue
      ]
    ) {
      def addOne(key: String, value: AttributeValue): Builder = {
        underlying.put(key, value)
        this
      }
      def addOne(key: ScalaString, value: AttributeValue): Builder = {
        underlying.put(AttributeValue.String(key), value)
        this
      }

      def ++=(entries: Iterable[(String, AttributeValue)]): Builder = {
        entries.foreach { case (k, v) => underlying.put(k, v) }
        this
      }

      def result: AttributeValue.Map =
        AttributeValue.Map(new JMapView(underlying, cloneFn))

      def clear(): Unit = underlying.clear()
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

  def encode[A](a: A)(implicit schema: Schema[A]): AttributeValue = Codec.encoder(schema)(a)

  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = scala.Predef.identity(_)
}
