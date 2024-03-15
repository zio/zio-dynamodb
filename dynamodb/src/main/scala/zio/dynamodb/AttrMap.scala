package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.dynamodb.DynamoDBError.ItemError
import zio.prelude.ForEachOps

final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  def toAttributeValue: AttributeValue =
    ToAttributeValue.attrMapToAttributeValue.toAttributeValue(self)

  def +(t: (String, AttributeValue)): AttrMap = AttrMap(map + t)

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[ItemError, A] =
    map
      .get(field)
      .toRight(DecodingError(s"field '$field' not found"))
      .flatMap(ev.fromAttributeValue)

  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    get(field) match {
      case Right(value) => Right(Some(value))
      case _            => Right(None)
    }

  def getItem[A](field: String)(f: AttrMap => Either[ItemError, A]): Either[ItemError, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[ItemError, A]): Either[ItemError, Option[A]] =
    getOptional[Item](field).flatMap(
      _.fold[Either[ItemError, Option[A]]](Right(None))(item => f(item).map(Some(_)))
    )

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](
    field: String
  )(f: AttrMap => Either[ItemError, A]): Either[ItemError, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[ItemError, Iterable[A]](xs => xs.forEach(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
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

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
