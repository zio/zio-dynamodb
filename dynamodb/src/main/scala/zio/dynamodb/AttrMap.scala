package zio.dynamodb

import io.github.vigoo.zioaws.dynamodb.model.primitives.AttributeName
import zio.dynamodb.DynamoDBExecutorImpl.awsAttributeValue
import io.github.vigoo.zioaws.dynamodb.model.AttributeValue

final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[String, A] =
    map
      .get(field)
      .toRight(s"field '$field' not found")
      .flatMap(ev.fromAttributeValue)

  def getOptional[A](field: String)(implicit ev: FromAttributeValue[A]): Either[Nothing, Option[A]] =
    get(field) match {
      case Right(value) => Right(Some(value))
      case _            => Right(None)
    }

  def getItem[A](field: String)(f: AttrMap => Either[String, A]): Either[String, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[String, A]): Either[String, Option[A]] =
    getOptional[Item](field).flatMap(_.fold[Either[String, Option[A]]](Right(None))(item => f(item).map(Some(_))))

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](field: String)(f: AttrMap => Either[String, A]): Either[String, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[String, Iterable[A]](xs => EitherUtil.forEach(xs)(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[String, A]): Either[String, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[String, Option[Iterable[A]]] =
      maybeItems match {
        case None     => Right(None)
        case Some(xs) => EitherUtil.forEach(xs)(f).map(Some(_))
      }
    getOptional[Iterable[Item]](field: String).flatMap(maybeTransform)
  }

  private[dynamodb] def toZioAwsMap(): Map[AttributeName, AttributeValue] =
    self.map.flatMap { case (k, v) => awsAttributeValue(v).map(a => (k, a)) }

}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
