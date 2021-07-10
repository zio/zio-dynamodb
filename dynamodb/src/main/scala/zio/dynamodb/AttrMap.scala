package zio.dynamodb

final case class AttrMap(map: Map[String, AttributeValue]) {
  def get[A](field: String)(implicit ev: FromAttributeValue[A]): Option[A]            =
    map.get(field).map(ev.fromAttributeValue).flatten
//  map.get(field) match {
//    case Some(AttributeValue.String(s)) => Some(s).asInstanceOf[Option[A]]
//    case Some(AttributeValue.Number(i)) => Some(i).asInstanceOf[Option[A]]
//  }
  def getOpt[A](field: String)(implicit ev: FromAttributeValue[A]): Option[Option[A]] =
    map.get(field).map(ev.fromAttributeValue)

}

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
