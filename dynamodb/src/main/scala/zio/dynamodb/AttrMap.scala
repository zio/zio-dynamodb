package zio.dynamodb

import zio.dynamodb.DynamoDBError.DecodingError

final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[DynamoDBError, A] =
    map
      .get(field)
      .toRight(DecodingError(s"field '$field' not found"))
      .flatMap(ev.fromAttributeValue)

  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    get(field) match {
      case Right(value) => Right(Some(value))
      case _            => Right(None)
    }

  def getItem[A](field: String)(f: AttrMap => Either[DynamoDBError, A]): Either[DynamoDBError, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[DynamoDBError, A]): Either[DynamoDBError, Option[A]] =
    getOptional[Item](field).flatMap(_.fold[Either[DynamoDBError, Option[A]]](Right(None))(item => f(item).map(Some(_))))

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](field: String)(f: AttrMap => Either[DynamoDBError, A]): Either[DynamoDBError, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[DynamoDBError, Iterable[A]](xs => EitherUtil.forEach(xs)(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[DynamoDBError, A]): Either[DynamoDBError, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[DynamoDBError, Option[Iterable[A]]] =
      maybeItems match {
        case None     => Right(None)
        case Some(xs) => EitherUtil.forEach(xs)(f).map(Some(_))
      }
    getOptional[Iterable[Item]](field: String).flatMap(maybeTransform)
  }
}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
