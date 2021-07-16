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

  implicit def mapFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Map[String, A]] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.Map(map) =>
          Some(map.toMap.map {
            case (avK, avV) =>
              avK.value -> ev
                .fromAttributeValue(avV)
                .get // this is safe as we should have implicits for all possible AttributeValue types
          })
        case _                       => None
      }

  implicit def stringSetFromAttributeValue: FromAttributeValue[Set[String]] =
    (av: AttributeValue) =>
      av match {
        case AttributeValue.StringSet(set) => Some(set)
        case _                             => None
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
