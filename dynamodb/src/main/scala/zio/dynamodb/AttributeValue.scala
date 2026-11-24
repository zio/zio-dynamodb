package zio.dynamodb

import zio.dynamodb.ConditionExpression.Operand._
import zio.dynamodb.ConditionExpression._
import zio.dynamodb.DynamoDBError.ItemError
import zio.schema.Schema
import scala.collection.immutable.Set
import scala.collection.immutable.{ Map => SMap }
import scala.util.Try

/**
 * A DynamoDB attribute value: a string, number, binary, boolean, null, list, map, or a set of strings, numbers or
 * binaries. It is the low level form of the data the executor sends to and reads from DynamoDB, and the form that
 * [[ToAttributeValue]] and [[FromAttributeValue]] convert to and from.
 *
 * `AttributeValue(a)` converts a single value with its [[ToAttributeValue]] instance, `AttributeValue.encode(a)` encodes
 * a whole value with its zio-schema `Schema`, and `decode` goes back.
 *
 * {{{
 * import zio.schema.{ DeriveSchema, Schema }
 *
 * final case class Point(x: Int, y: Int)
 * object Point {
 *   implicit val schema: Schema.CaseClass2[Int, Int, Point] = DeriveSchema.gen[Point]
 * }
 *
 * val n: AttributeValue    = AttributeValue(42)             // Number(42)
 * val tags: AttributeValue = AttributeValue(Set("a", "b"))  // StringSet(Set(a, b))
 * val p: AttributeValue    = AttributeValue.encode(Point(1, 2))
 * // Map(Map(String(x) -> Number(1), String(y) -> Number(2)))
 * val back = p.decode[Point] // Right(Point(1,2))
 * val item = p.toAttrMap     // Right(AttrMap(Map(x -> Number(1), y -> Number(2))))
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/reference/low-level-api/attribute-value/ AttributeValue]]
 */
sealed trait AttributeValue { self =>

  /**
   * The Scala type a value was converted from. `AttributeValue(a)` returns an `AttributeValue.WithScalaType[A]`, which
   * fixes it to the type of `a`; for other values it is abstract.
   */
  type ScalaType

  /**
   * Returns this value as an [[AttrMap]] when it is a map, or a `DecodingError` otherwise (for example
   * `Error getting AttrMap value. Expected AttributeValue.Map but found AttributeValue.Number`).
   */
  def toAttrMap: Either[ItemError, AttrMap] =
    FromAttributeValue.attrMapFromAttributeValue.fromAttributeValue(self)

  /** Decodes this value into an `A` with the codec derived from `schema`, or returns the `ItemError` that stopped it. */
  def decode[A](implicit schema: Schema[A]): Either[ItemError, A] = Codec.decoder(schema)(self)

  /**
   * Builds the condition `value = size(path)`. Currently the size operand must have this value's `ScalaType` as its
   * attribute type, so `AttributeValue(3) === Person.tags.size` does not compile; write `Person.tags.size === 3`.
   */
  def ===[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] = Equals(ValueOperand(self), that)

  /**
   * Builds the condition `value <> size(path)`. Currently the size operand must have this value's `ScalaType` as its
   * attribute type, so a number value does not compile with it; write `path.size <> n`.
   */
  def <>[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] = NotEqual(ValueOperand(self), that)

  /**
   * Builds the condition `value < size(path)`. Currently the size operand must have this value's `ScalaType` as its
   * attribute type, so a number value does not compile with it; write `path.size > n`.
   */
  def <[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] =
    LessThan(ValueOperand(self), that)

  /**
   * Builds the condition `value <= size(path)`. Currently the size operand must have this value's `ScalaType` as
   * its attribute type, so a number value does not compile with it; write `path.size >= n`.
   */
  def <=[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] =
    LessThanOrEqual(ValueOperand(self), that)

  /**
   * Meant to build the condition `value > size(path)` for the size of an untyped path, such as
   * `AttributeValue(3) > \$("tags").size`. It currently builds `value >= size(path)` instead.
   */
  def >[From](that: Operand.Size[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    GreaterThanOrEqual(ValueOperand(self), that)

  /**
   * Builds the condition `value >= size(path)`. Currently the size operand must have this value's `ScalaType` as
   * its attribute type, so a number value does not compile with it; write `path.size <= n`.
   */
  def >=[From](that: Operand.Size[From, ScalaType]): ConditionExpression[From] =
    GreaterThanOrEqual(ValueOperand(self), that)

  /** Builds the condition `value = path` for an untyped path, such as `AttributeValue(3) === \$("count")`. */
  def ===[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    Equals(ValueOperand(self), ProjectionExpressionOperand(that))

  /** Builds the condition `value <> path` for an untyped path. */
  def <>[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    NotEqual(ValueOperand(self), ProjectionExpressionOperand(that))

  /** Builds the condition `value < path` for an untyped path. */
  def <[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    LessThan(ValueOperand(self), ProjectionExpressionOperand(that))

  /** Builds the condition `value <= path` for an untyped path. */
  def <=[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    LessThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))

  /**
   * Meant to build the condition `value > path` for an untyped path. It currently builds `value >= path` instead.
   */
  def >[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
    GreaterThanOrEqual(ValueOperand(self), ProjectionExpressionOperand(that))

  /** Builds the condition `value >= path` for an untyped path. */
  def >=[From](that: ProjectionExpression[From, ProjectionExpression.Unknown]): ConditionExpression[From] =
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

/**
 * Builds [[AttributeValue]] values from Scala values: `apply` with a [[ToAttributeValue]] instance, and `encode` with a
 * zio-schema `Schema`.
 */
object AttributeValue {
  import Predef.{ String => ScalaString }
  import scala.collection.immutable.{ Map => ScalaMap }

  /** An [[AttributeValue]] whose `ScalaType` is `X`, the type `AttributeValue.apply` returns. */
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

  /**
   * Converts `a` with its [[ToAttributeValue]] instance: `AttributeValue(42)` is `Number(42)` and
   * `AttributeValue(Set("a", "b"))` is `StringSet(Set(a, b))`.
   */
  def apply[A](a: A)(implicit ev: ToAttributeValue[A]): AttributeValue.WithScalaType[A] =
    ev.toAttributeValue(a).asInstanceOf[AttributeValue.WithScalaType[A]]

  /**
   * Encodes `a` with the codec derived from `schema`: a case class becomes a map with one entry per field, except that
   * a field holding `None` is left out, and a `None` on its own becomes `Null`.
   */
  def encode[A](a: A)(implicit schema: Schema[A]): AttributeValue = Codec.encoder(schema)(a)

  /**
   * The [[ToAttributeValue]] of `AttributeValue` itself, which returns the value unchanged, so an `AttributeValue` can
   * be passed where a convertible value is expected. It applies to values typed as `AttributeValue`, not to the
   * `AttributeValue.WithScalaType[A]` that `AttributeValue(a)` returns.
   */
  implicit val attributeValueToAttributeValue: ToAttributeValue[AttributeValue] = scala.Predef.identity(_)
}
