package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.DynamoDBError.ItemError
import zio.schema.Schema
import scala.collection.immutable.Set
import scala.collection.immutable.{ Map => SMap }
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

  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue { self =>
    def +(t: (ScalaString, AttributeValue)): Map = {
      val (s, av) = t
      Map(self.value + ((String(s), av)))
    }
  }

  private[dynamodb] object Map {
    val empty = Map(ScalaMap.empty)

    /**
     * An immutable Scala Map view over an underlying Java Map - for faster Map construction
     */
    private[dynamodb] final class JMapView(
      private val underlying: java.util.Map[String, AttributeValue],
      private val cloneFn: java.util.Map[String, AttributeValue] => java.util.Map[String, AttributeValue]
    ) extends scala.collection.immutable.AbstractMap[String, AttributeValue] {

      override def get(key: String): Option[AttributeValue] = {
        val v = underlying.get(key)
        if (v eq null) None else Some(v)
      }

      def getNullable(key: String): AttributeValue =
        underlying.get(key)

      override def iterator: Iterator[(String, AttributeValue)] = {
        import scala.jdk.CollectionConverters._
        underlying.entrySet().asScala.iterator.map(e => (e.getKey, e.getValue))
      }

      override def removed(key: String): JMapView = {
        val copy: java.util.Map[String, AttributeValue] = cloneFn(underlying)
        copy.remove(key)
        new JMapView(copy, cloneFn)
      }

      override def updated[A >: AttributeValue](key: String, value: A): JMapView = {
        val copy: java.util.Map[String, AttributeValue] = cloneFn(underlying)
        copy.put(key, value.asInstanceOf[AttributeValue])
        new JMapView(copy, cloneFn)
      }

    }

    object JMapView {
      object hash {
        def builder: Builder =
          new Builder(
            new java.util.HashMap[String, AttributeValue](),
            m => new java.util.HashMap[String, AttributeValue](m)
          )

        def single(key: ScalaString, value: AttributeValue): SMap[String, AttributeValue] = {
          val map = new java.util.HashMap[String, AttributeValue](1)
          map.put(String(key), value)
          new JMapView(map, m => new java.util.HashMap[String, AttributeValue](m))
        }

      }

      object linked {
        def builder: Builder =
          new Builder(
            new java.util.LinkedHashMap[String, AttributeValue](),
            m => new java.util.LinkedHashMap[String, AttributeValue](m)
          )

        def single(key: ScalaString, value: AttributeValue): SMap[String, AttributeValue] = {
          val map = new java.util.LinkedHashMap[String, AttributeValue](1)
          map.put(String(key), value)
          new JMapView(map, m => new java.util.LinkedHashMap[String, AttributeValue](m))
        }

      }

      private[dynamodb] final class Builder(
        private val underlying: java.util.Map[String, AttributeValue],
        private val cloneFn: java.util.Map[String, AttributeValue] => java.util.Map[
          String,
          AttributeValue
        ]
      ) { self =>

        def addOne(key: ScalaString, value: AttributeValue): Builder = {
          underlying.put(String(key), value)
          self
        }

        def ++=(entries: Iterable[(String, AttributeValue)]): Builder = {
          entries.foreach { case (k, v) => underlying.put(k, v) }
          self
        }

        def result: SMap[String, AttributeValue] =
          new JMapView(underlying, cloneFn)

        def clear(): Unit = underlying.clear()
      }

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
