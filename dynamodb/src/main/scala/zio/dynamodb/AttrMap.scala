package zio.dynamodb

import zio.dynamodb.DynamoDBError.DecodingError
import zio.prelude.ForEachOps

final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[DynamoDBItemError, A] =
    map
      .get(field)
      .toRight(DecodingError(s"field '$field' not found"))
      .flatMap(ev.fromAttributeValue)

  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    get(field) match {
      case Right(value) => Right(Some(value))
      case _            => Right(None)
    }

  def getItem[A](field: String)(f: AttrMap => Either[DynamoDBItemError, A]): Either[DynamoDBItemError, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[DynamoDBItemError, A]): Either[DynamoDBItemError, Option[A]] =
    getOptional[Item](field).flatMap(
      _.fold[Either[DynamoDBItemError, Option[A]]](Right(None))(item => f(item).map(Some(_)))
    )

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](field: String)(f: AttrMap => Either[DynamoDBItemError, A]): Either[DynamoDBItemError, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[DynamoDBItemError, Iterable[A]](xs => xs.forEach(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[DynamoDBItemError, A]): Either[DynamoDBItemError, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[DynamoDBItemError, Option[Iterable[A]]] =
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
