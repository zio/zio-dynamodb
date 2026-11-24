package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.DynamoDBError.ItemError
import zio.prelude.ForEachOps

/** A type class that decodes an [[AttributeValue]] into a value of type `A`. */
trait FromAttributeValue[+A] { // TODO: Avi replace ItemError with DecodingError
  /**
   * Decodes `av`. Returns a `Left` with an `ItemError` when `av` is not of the expected type; the instances in the
   * companion object return a `DecodingError`.
   */
  def fromAttributeValue(av: AttributeValue): Either[ItemError, A]
}

/**
 * Instances of [[FromAttributeValue]] for `String`, `Boolean`, `Byte`, the numeric types and sets of them, byte
 * collections, `Option`, `Map[String, A]`, `Iterable[A]` and [[AttrMap]].
 */
object FromAttributeValue {

  /** Returns the implicit instance of [[FromAttributeValue]] for `A`. */
  def apply[A](implicit from: FromAttributeValue[A]): FromAttributeValue[A] = from

  /**
   * Decodes `AttributeValue.Null` as `None`, and any other value as `Some` of what the instance for `A` decodes, or the
   * error that instance returns.
   */
  implicit def optionFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Option[A]] = {
    case AttributeValue.Null =>
      Right(None)
    case av: AttributeValue  =>
      ev.fromAttributeValue(av).map(Some(_))
  }

  /** Decodes an `AttributeValue.Binary` as its bytes. Returns a `Left` for other types. */
  implicit val binaryFromAttributeValue: FromAttributeValue[Iterable[Byte]] = {
    case AttributeValue.Binary(b) => Right(b)
    case av                       =>
      Left(DecodingError(s"Error getting binary value. Expected AttributeValue.Binary but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Binary` as its first byte. Returns a `Left` when the binary value is empty, and a `Left`
   * for other types.
   */
  implicit val byteFromAttributeValue: FromAttributeValue[Byte] = {
    case AttributeValue.Binary(b) => b.headOption.toRight(DecodingError("Error: byte array is empty"))
    case av                       => Left(DecodingError(s"Error getting byte value. Expected AttributeValue.Binary but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.BinarySet` as a collection of byte collections. Returns a `Left` for other types. */
  implicit def binarySetFromAttributeValue: FromAttributeValue[Iterable[Iterable[Byte]]] = {
    case AttributeValue.BinarySet(set) => Right(set)
    case av                            =>
      Left(DecodingError(s"Error getting binary set value. Expected AttributeValue.BinarySet but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.Bool` as a `Boolean`. Returns a `Left` for other types. */
  implicit val booleanFromAttributeValue: FromAttributeValue[Boolean] = {
    case AttributeValue.Bool(b) => Right(b)
    case av                     =>
      Left(DecodingError(s"Error getting boolean value. Expected AttributeValue.Bool but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.String` as a `String`. Returns a `Left` for other types. */
  implicit val stringFromAttributeValue: FromAttributeValue[String] = {
    case AttributeValue.String(s) => Right(s)
    case av                       =>
      Left(DecodingError(s"Error getting string value. Expected AttributeValue.String but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Number` as a `Short` with `BigDecimal#shortValue`, so a number outside the `Short` range
   * wraps around (70000 becomes 4464) and a fraction is dropped, without an error. Returns a `Left` for other types.
   */
  implicit val shortFromAttributeValue: FromAttributeValue[Short] = {
    case AttributeValue.Number(bd) => Right(bd.shortValue)
    case av                        =>
      Left(DecodingError(s"Error getting short value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.NumberSet` as a `Set[Short]` with `BigDecimal#shortValue` on each element, which wraps
   * out-of-range numbers around. Returns a `Left` for other types.
   */
  implicit val shortSetFromAttributeValue: FromAttributeValue[Set[Short]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.shortValue))
    case av                              =>
      Left(DecodingError(s"Error getting short set value. Expected AttributeValue.NumberSet but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Number` as an `Int` with `BigDecimal#intValue`, so a number outside the `Int` range
   * wraps around and a fraction is dropped (3.7 becomes 3), without an error. Returns a `Left` for other types.
   */
  implicit val intFromAttributeValue: FromAttributeValue[Int] = {
    case AttributeValue.Number(bd) => Right(bd.intValue)
    case av                        => Left(DecodingError(s"Error getting int value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.NumberSet` as a `Set[Int]` with `BigDecimal#intValue` on each element, which wraps
   * out-of-range numbers around and drops fractions. Returns a `Left` for other types.
   */
  implicit val intSetFromAttributeValue: FromAttributeValue[Set[Int]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.intValue))
    case av                              =>
      Left(DecodingError(s"Error getting int set value. Expected AttributeValue.NumberSet but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Number` as a `Long` with `BigDecimal#longValue`, so a number outside the `Long` range
   * wraps around and a fraction is dropped, without an error. Returns a `Left` for other types.
   */
  implicit val longFromAttributeValue: FromAttributeValue[Long] = {
    case AttributeValue.Number(bd) => Right(bd.longValue)
    case av                        => Left(DecodingError(s"Error getting long value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.NumberSet` as a `Set[Long]` with `BigDecimal#longValue` on each element, which wraps
   * out-of-range numbers around and drops fractions. Returns a `Left` for other types.
   */
  implicit val longSetFromAttributeValue: FromAttributeValue[Set[Long]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.longValue))
    case av                              =>
      Left(DecodingError(s"Error getting long set value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Number` as a `Float` with `BigDecimal#floatValue`, which can lose precision and gives
   * `Infinity` for a number that is too large. Returns a `Left` for other types.
   */
  implicit val floatFromAttributeValue: FromAttributeValue[Float] = {
    case AttributeValue.Number(bd) => Right(bd.floatValue)
    case av                        =>
      Left(DecodingError(s"Error getting float value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.NumberSet` as a `Set[Float]` with `BigDecimal#floatValue` on each element. Returns a
   * `Left` for other types.
   */
  implicit val floatSetFromAttributeValue: FromAttributeValue[Set[Float]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.floatValue))
    case av                              =>
      Left(DecodingError(s"Error getting float set value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.Number` as a `Double` with `BigDecimal#doubleValue`, which can lose precision. Returns a
   * `Left` for other types.
   */
  implicit val doubleFromAttributeValue: FromAttributeValue[Double] = {
    case AttributeValue.Number(bd) => Right(bd.doubleValue)
    case av                        =>
      Left(DecodingError(s"Error getting double value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.NumberSet` as a `Set[Double]` with `BigDecimal#doubleValue` on each element. Returns a
   * `Left` for other types.
   */
  implicit val doubleSetFromAttributeValue: FromAttributeValue[Set[Double]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.doubleValue))
    case av                              =>
      Left(DecodingError(s"Error getting double value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.Number` as a `BigDecimal`. Returns a `Left` for other types. */
  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal] = {
    case AttributeValue.Number(bd) => Right(bd)
    case av                        =>
      Left(DecodingError(s"Error getting BigDecimal value. Expected AttributeValue.Number but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.NumberSet` as a `Set[BigDecimal]`. Returns a `Left` for other types. */
  implicit val bigDecimalSetFromAttributeValue: FromAttributeValue[Set[BigDecimal]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet)
    case av                              =>
      Left(
        DecodingError(s"Error getting BigDecimal set value. Expected AttributeValue.Number but found ${av.showType}")
      )
  }

  /**
   * Decodes an `AttributeValue.Map` as a `Map[String, A]`, decoding each value with the instance for `A`. Returns a
   * `Left` when a value fails to decode, and a `Left` for other types.
   */
  implicit def mapFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Map[String, A]] = {
    case AttributeValue.Map(map) =>
      map.toList.forEach { case (avK, avV) =>
        ev.fromAttributeValue(avV).map(v => (avK.value, v))
      }
        .map(_.toMap)
    case av                      => Left(DecodingError(s"Error getting map value. Expected AttributeValue.Map but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.StringSet` as a `Set[String]`. Returns a `Left` for other types. */
  implicit def stringSetFromAttributeValue: FromAttributeValue[Set[String]] = {
    case AttributeValue.StringSet(set) => Right(set)
    case av                            =>
      Left(DecodingError(s"Error getting string set value. Expected AttributeValue.StringSet but found ${av.showType}"))
  }

  /** Decodes an `AttributeValue.Map` as an [[AttrMap]]. Returns a `Left` for other types. */
  implicit val attrMapFromAttributeValue: FromAttributeValue[AttrMap] = {
    case AttributeValue.Map(map) =>
      Right(new AttrMap(map.toMap.map { case (k, v) => k.value -> v }))
    case av                      => Left(DecodingError(s"Error getting AttrMap value. Expected AttributeValue.Map but found ${av.showType}"))
  }

  /**
   * Decodes an `AttributeValue.List` as a `Chunk` of `A`, decoding each element with the instance for `A`. Returns a
   * `Left` when an element fails to decode, and a `Left` for other types. Summoning
   * `FromAttributeValue[Iterable[Int]]` or `FromAttributeValue[Iterable[String]]` currently picks the `Set` instance,
   * which fails on an `AttributeValue.List`; call `iterableFromAttributeValue[A]` explicitly to get this instance.
   */
  implicit def iterableFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Iterable[A]] = {
    case AttributeValue.List(list) =>
      list.forEach(ev.fromAttributeValue)
    case av                        =>
      Left(DecodingError(s"Error getting iterable value. Expected AttributeValue.List but found ${av.showType}"))
  }

}
