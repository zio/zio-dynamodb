package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.DynamoDBError.ItemError
import zio.prelude.ForEachOps

/**
 * A DynamoDB item or key in its low level form: attribute names mapped to [[AttributeValue]]s. `Item` and
 * `PrimaryKey` are other names for this type. `AttrMap(...)` builds one from pairs whose values have a
 * [[ToAttributeValue]], and `get` reads a field back with its [[FromAttributeValue]].
 *
 * {{{
 * val user = AttrMap("id" -> "u-17", "age" -> 30)
 *
 * val age     = user.get[Int]("age")             // Right(30)
 * val missing = user.get[Int]("email").isLeft    // true
 * val nick    = user.getOptional[String]("nick") // Right(None)
 * val older   = user + ("age" -> AttributeValue(31))
 * // AttrMap(Map(id -> String(u-17), age -> Number(31)))
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/reference/low-level-api/attribute-value/ AttributeValue and AttrMap]]
 */
final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  /** Returns this map as an [[AttributeValue]] map, for nesting it inside another item. */
  def toAttributeValue: AttributeValue =
    ToAttributeValue.attrMapToAttributeValue.toAttributeValue(self)

  /** Returns a copy with the attribute `t`, replacing any attribute of the same name. */
  def +(t: (String, AttributeValue)): AttrMap = AttrMap(map + t)

  /**
   * Returns the attribute `field` decoded as an `A`, or a `DecodingError` when the attribute is missing (for `email`
   * the message is `field 'email' not found`) or cannot be decoded as an `A`.
   */
  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[ItemError, A] =
    map
      .get(field)
      .toRight(DecodingError(s"field '$field' not found"))
      .flatMap(ev.fromAttributeValue)

  /**
   * Returns the attribute `field` decoded as an `A` in a `Some`, or `None` when the attribute is missing. It never
   * fails: currently it also returns `None` when the attribute is present but cannot be decoded as an `A`.
   */
  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    get(field) match {
      case Right(value) => Right(Some(value))
      case _            => Right(None)
    }

  /**
   * Reads the nested map `field` and converts it with `f`. Fails when the field is missing or is not a map, or when `f`
   * fails.
   */
  def getItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  /**
   * Reads the nested map `field` and converts it with `f`, or returns `None` when the field is missing. Fails when `f`
   * fails. As with `getOptional`, a field that is present but not a map also gives `None`.
   */
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[ItemError, A]): Either[ItemError, Option[A]] =
    getOptional[Item](field).flatMap(
      _.fold[Either[ItemError, Option[A]]](Right(None))(item => f(item).map(Some(_)))
    )

  // convenience method so that user does not have to transform between a List and an Either
  /**
   * Reads the list of maps `field` and converts each element with `f`. Fails when the field is missing or is not a list
   * of maps, or when `f` fails for an element.
   */
  def getIterableItem[A](
    field: String
  )(f: AttrMap => Either[ItemError, A]): Either[ItemError, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[ItemError, Iterable[A]](xs => xs.forEach(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  /**
   * Reads the list of maps `field` and converts each element with `f`, or returns `None` when the field is missing.
   * Fails when `f` fails for an element. As with `getOptional`, a field that is present but not a list of maps also
   * gives `None`.
   */
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[ItemError, A]): Either[ItemError, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[ItemError, Option[Iterable[A]]] =
      maybeItems match {
        case None     => Right(None)
        case Some(xs) => xs.forEach(f).map(Some(_))
      }
    getOptional[Iterable[Item]](field: String).flatMap(maybeTransform)
  }
}

/**
 * Builds [[AttrMap]] values. Besides `AttrMap(map)`, its `apply` methods take from 1 to 22 `(name, value)` pairs whose
 * values have a [[ToAttributeValue]].
 */
object AttrMap extends GeneratedAttrMapApplies {

  /** The map with no attributes. */
  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
