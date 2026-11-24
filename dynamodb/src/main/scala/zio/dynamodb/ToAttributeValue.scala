package zio.dynamodb

import zio.Chunk
import zio.schema.Schema
import scala.collection.immutable.{ Map, Set }

/** A type class that encodes a value of type `A` as an [[AttributeValue]]. */
trait ToAttributeValue[A] {

  /** Encodes `a` as an [[AttributeValue]]. */
  def toAttributeValue(a: A): AttributeValue
}

/**
 * Instances of [[ToAttributeValue]] for `String`, `Boolean`, `Byte`, the numeric types and sets of them, byte
 * collections, `Option`, `Map[String, A]` and [[AttrMap]]. Collections and types with a ZIO Schema are covered by
 * lower-priority instances.
 */
object ToAttributeValue extends ToAttributeValueLowPriorityImplicits0 {
  import Predef.{ String => ScalaString }

  /** Returns the implicit instance of [[ToAttributeValue]] for `A`. */
  def apply[A](implicit to: ToAttributeValue[A]): ToAttributeValue[A] = to

  /**
   * Encodes `None` as `AttributeValue.Null` and `Some(a)` as the encoding of `a`, so `Some(None)` is also encoded as
   * `AttributeValue.Null`.
   */
  implicit def optionToAttributeValue[A](implicit ev: ToAttributeValue[A]): ToAttributeValue[Option[A]] = {
    case None    => AttributeValue.Null
    case Some(a) => ev.toAttributeValue(a)
  }

  /** Encodes a collection of bytes, such as a `Chunk[Byte]` or a `List[Byte]`, as `AttributeValue.Binary`. */
  implicit def binaryToAttributeValue[Col[A] <: Iterable[A], A <: Byte]: ToAttributeValue[Col[A]] =
    x => AttributeValue.Binary(x)

  /** Encodes a single byte as an `AttributeValue.Binary` that holds one byte. */
  implicit def byteToAttributeValue: ToAttributeValue[Byte] =
    a => AttributeValue.Binary(Chunk(a))

  /**
   * Encodes a collection of byte collections, such as a `Set[Set[Byte]]` or a `List[Chunk[Byte]]`, as
   * `AttributeValue.BinarySet`.
   */
  implicit def binarySetToAttributeValue[Col1[A] <: Iterable[A], Col2[B] <: Iterable[B], B <: Byte]
    : ToAttributeValue[Col1[Col2[B]]] = AttributeValue.BinarySet(_)

  /** Encodes a `Boolean` as `AttributeValue.Bool`. */
  implicit val boolToAttributeValue: ToAttributeValue[Boolean] = AttributeValue.Bool(_)

  /** Encodes an [[AttrMap]] as `AttributeValue.Map`, with each key as an `AttributeValue.String`. */
  implicit val attrMapToAttributeValue: ToAttributeValue[AttrMap] =
    (attrMap: AttrMap) =>
      AttributeValue.Map {
        attrMap.map.map { case (key, value) =>
          (AttributeValue.String(key), value)
        }
      }

  /** Encodes a `Map[String, A]` as `AttributeValue.Map`, encoding each value with the instance for `A`. */
  implicit def mapToAttributeValue[A](implicit ev: ToAttributeValue[A]): ToAttributeValue[Map[ScalaString, A]] =
    (map: Map[ScalaString, A]) =>
      AttributeValue.Map(map.map { case (k, v) => (AttributeValue.String(k), ev.toAttributeValue(v)) })

  /** Encodes a `String` as `AttributeValue.String`. */
  implicit val stringToAttributeValue: ToAttributeValue[ScalaString]                                           = AttributeValue.String(_)

  /** Encodes a `Set[String]` as `AttributeValue.StringSet`. */
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]] =
    AttributeValue.StringSet(_)
  // BigDecimal support
  /** Encodes a `BigDecimal` as `AttributeValue.Number`. */
  implicit val bigDecimalToAttributeValue: ToAttributeValue[BigDecimal]      = AttributeValue.Number(_)

  /** Encodes a `Set[BigDecimal]` as `AttributeValue.NumberSet`. */
  implicit val bigDecimalSetToAttributeValue: ToAttributeValue[Set[BigDecimal]] = AttributeValue.NumberSet(_)
  // short support
  /** Encodes a `Short` as `AttributeValue.Number`. */
  implicit val shortToAttributeValue: ToAttributeValue[Short]                   = (a: Short) =>
    AttributeValue.Number(BigDecimal(a.toString))

  /** Encodes a `Set[Short]` as `AttributeValue.NumberSet`. */
  implicit val shortSetToAttributeValue: ToAttributeValue[Set[Short]] = (a: Set[Short]) =>
    AttributeValue.NumberSet(a.map(s => BigDecimal(s.toString)))

  // Int support
  /** Encodes an `Int` as `AttributeValue.Number`. */
  implicit val intToAttributeValue: ToAttributeValue[Int] = (a: Int) => AttributeValue.Number(BigDecimal(a))

  /** Encodes a `Set[Int]` as `AttributeValue.NumberSet`. */
  implicit val intSetToAttributeValue: ToAttributeValue[Set[Int]] = (a: Set[Int]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Long support
  /** Encodes a `Long` as `AttributeValue.Number`. */
  implicit val longToAttributeValue: ToAttributeValue[Long]       = (a: Long) => AttributeValue.Number(BigDecimal(a))

  /** Encodes a `Set[Long]` as `AttributeValue.NumberSet`. */
  implicit val longSetToAttributeValue: ToAttributeValue[Set[Long]] = (a: Set[Long]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Double support
  /** Encodes a `Double` as `AttributeValue.Number`. */
  implicit val doubleToAttributeValue: ToAttributeValue[Double]     = (a: Double) => AttributeValue.Number(BigDecimal(a))

  /** Encodes a `Set[Double]` as `AttributeValue.NumberSet`. */
  implicit val doubleSetToAttributeValue: ToAttributeValue[Set[Double]] = (a: Set[Double]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Float support
  /** Encodes a `Float` as `AttributeValue.Number` using its shortest decimal form, so `0.1f` is stored as `0.1`. */
  implicit val floatToAttributeValue: ToAttributeValue[Float]           = (a: Float) =>
    AttributeValue.Number(BigDecimal.decimal(a))

  /** Encodes a `Set[Float]` as `AttributeValue.NumberSet` using the shortest decimal form of each element. */
  implicit val floatSetToAttributeValue: ToAttributeValue[Set[Float]] = (a: Set[Float]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.decimal))

}

/**
 * Lower-priority instances of [[ToAttributeValue]] for collections. The companion object extends it, so the instances
 * for byte collections and for sets of strings and numbers are chosen first.
 */
trait ToAttributeValueLowPriorityImplicits0 extends ToAttributeValueLowPriorityImplicits1 {

  /**
   * Encodes a collection such as a `List` or a `Vector` as `AttributeValue.List`, encoding each element with the
   * instance for `A`.
   */
  implicit def collectionToAttributeValue[Col[X] <: Iterable[X], A](implicit
    element: ToAttributeValue[A]
  ): ToAttributeValue[Col[A]] =
    (xs: Col[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))

}

/** Lowest-priority instances of [[ToAttributeValue]]: `Null` and the ZIO Schema based encoder. */
trait ToAttributeValueLowPriorityImplicits1 {

  /** Encodes `null` as `AttributeValue.Null`. */
  implicit val nullToAttributeValue: ToAttributeValue[Null] = (_: Null) => AttributeValue.Null

  /**
   * Encodes a value with the DynamoDB encoder derived from the implicit `Schema[A]`. A case class with a schema is
   * encoded as an `AttributeValue.Map` with one entry per field, except that a field holding `None` is left out of the
   * map. A `None` on its own is encoded as `AttributeValue.Null`.
   */
  implicit def schemaToAttributeValue[A](implicit schema: Schema[A]): ToAttributeValue[A] =
    (a: A) => {
      val enc = Codec.encoder(schema)
      enc(a)
    }

}
