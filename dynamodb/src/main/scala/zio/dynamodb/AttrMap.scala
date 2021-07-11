package zio.dynamodb

final case class AttrMap(map: Map[String, AttributeValue]) {

  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Either[String, A] =
    map.get(field).map(ev.fromAttributeValue).flatten.toRight(s"field '$field' not found")

  def getOpt[A](field: String)(implicit ev: FromAttributeValue[A]): Option[A] =
    map.get(field).map(ev.fromAttributeValue).flatten

//  def getOptAttrMap[A, B](
//    field: String
//  )(f: AttrMap => Either[String, Option[B]])(implicit ev: FromAttributeValue[A]): Option[A] =
//    getOpt[A](field).fold[Either[String, Option[B]]](Right(None))(f)
}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
