package zio.dynamodb

trait FromAttributeValue[+A] {
  def fromAttributeValue(av: AttributeValue): Option[A]
}

object FromAttributeValue {

  implicit val stringFromAttributeValue2: FromAttributeValue[String] = (av: AttributeValue) =>
    av match {
      case AttributeValue.String(s) => Some(s)
      case _                        => None
    }

  implicit val intFromAttributeValue: FromAttributeValue[Int] = (av: AttributeValue) =>
    av match {
      case AttributeValue.Number(bd) => Some(bd.intValue)
      case _                         => None
    }

  implicit val booleanDecimalFromAttributeValue: FromAttributeValue[Boolean] = (av: AttributeValue) =>
    av match {
      case AttributeValue.Bool(b) => Some(b)
      case _                      => None
    }

  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal] = (av: AttributeValue) =>
    av match {
      case AttributeValue.Number(bd) => Some(bd)
      case _                         => None
    }

  // TODO: test ordinary Maps that are not AttrMaps
  implicit val mapFromAttributeValue: FromAttributeValue[AttributeValue.Map] = (av: AttributeValue) =>
    av match {
      case map @ AttributeValue.Map(_) => Some(map)
      case _                           => None
    }

  implicit val attrMapFromAttributeValue: FromAttributeValue[AttrMap] = (av: AttributeValue) => {
    av match {
      case AttributeValue.Map(map) =>
        Some(new AttrMap(map.toMap.map { case (k, v) => k.value -> v }))
      case _                       => None
    }
  }

  implicit def iterableFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Iterable[A]] =
    (av: AttributeValue) => {
      av match {
        case AttributeValue.List(list) =>
          Some(
            list.map(x => ev.fromAttributeValue(x).get)
          ) // TODO: ev.fromAttributeValue(x) returns an Option - what if its Null?
        case _                         => None
      }
    }

}
