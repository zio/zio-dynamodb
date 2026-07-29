package zio.dynamodb

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import Utils.ListUtils

import java.util.{ HashMap => JHashMap }

final case class AttrMap(map: Map[String, AttributeValue]) extends GeneratedFromAttributeValueAs { self =>

  def toAttributeValue: AttributeValue =
    ToAttributeValue.attrMapToAttributeValue.toAttributeValue(self)

  private[dynamodb] def foreachEntry(f: (String, AttributeValue) => Unit): Unit =
    map match {
      case jmv: JMapView[_, _] =>
        val it = jmv.underlying.entrySet().iterator()
        while (it.hasNext) {
          val e = it.next(); f(e.getKey.asInstanceOf[String], e.getValue.asInstanceOf[AttributeValue])
        }
      case m                   =>
        m.foreach { case (k, v) => f(k, v) }
    }

  def +(t: (String, AttributeValue)): AttrMap = AttrMap(map + t)

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[DecodingError, A] =
    map
      .get(field)
      .toRight(DecodingError(s"field '$field' not found"))
      .flatMap(ev.fromAttributeValue)

  def getOption[A](field: String)(implicit ev: FromAttributeValue[A]): Option[A] =
    get(field).toOption

  def getItem[A](field: String)(f: AttrMap => Either[DecodingError, A]): Either[DecodingError, A] =
    get[Item](field).flatMap(item => f(item))

  // convenience method so that user does not have to transform between an Option and an Either
  def getOptionalItem[A](
    field: String
  )(f: AttrMap => Either[DecodingError, A]): Either[ItemError, Option[A]] =
    getOption[Item](field).fold[Either[ItemError, Option[A]]](Right(None))(item => f(item).map(Some(_)))

  // convenience method so that user does not have to transform between a List and an Either
  def getIterableItem[A](
    field: String
  )(f: AttrMap => Either[DecodingError, A]): Either[ItemError, Iterable[A]] =
    get[Iterable[Item]](field).flatMap[DecodingError, Iterable[A]](xs => xs.forEach(f))

  // convenience method so that user does not have to transform between an Option, List and an Either
  def getOptionalIterableItem[A](
    field: String
  )(f: AttrMap => Either[DecodingError, A]): Either[ItemError, Option[Iterable[A]]] = {
    def maybeTransform(maybeItems: Option[Iterable[Item]]): Either[ItemError, Option[Iterable[A]]] =
      maybeItems match {
        case None     => Right(None)
        case Some(xs) => xs.forEach(f).map(Some(_))
      }
    maybeTransform(getOption[Iterable[Item]](field))
  }
}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

  // Wraps a pre-built Java HashMap without copying it into a Scala HashMap.
  // Used by AwsCodecs to avoid the toMap allocation on the decode hot-path.
  private[dynamodb] def fromJavaMap(jm: JHashMap[String, AttributeValue]): AttrMap =
    AttrMap(new zio.dynamodb.JMapView[String, AttributeValue](jm, m => new JHashMap[String, AttributeValue](m)))

}
