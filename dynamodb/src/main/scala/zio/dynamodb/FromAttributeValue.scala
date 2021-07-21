package zio.dynamodb

trait FromAttributeValue[+A] {
  // TODO: is a partial function the best way to capture this?
  def fromAttributeValue(av: AttributeValue): Option[A]
}

object FromAttributeValue {

  // TODO:
  implicit def optionFromAttributeValue2[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Option[A]] = ???

  implicit val stringFromAttributeValue2: FromAttributeValue[String] = {
    case AttributeValue.String(s) => Some(s)
    case _                        => None
  }

  implicit val intFromAttributeValue: FromAttributeValue[Int] = {
    case AttributeValue.Number(bd) => Some(bd.intValue)
    case _                         => None
  }

  implicit val booleanDecimalFromAttributeValue: FromAttributeValue[Boolean] = {
    case AttributeValue.Bool(b) => Some(b)
    case _                      => None
  }

  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal] = {
    case AttributeValue.Number(bd) => Some(bd)
    case _                         => None
  }

  implicit def mapFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Map[String, A]] = {
    case AttributeValue.Map(map) =>
      Some(map.toMap.map {
        case (avK, avV) =>
          avK.value -> ev
            .fromAttributeValue(avV)
            .get // this is safe as we should have implicits for all possible AttributeValue types
      })
    case _                       => None
  }

  implicit def stringSetFromAttributeValue: FromAttributeValue[Set[String]] = {
    case AttributeValue.StringSet(set) => Some(set)
    case _                             => None
  }

  implicit val attrMapFromAttributeValue: FromAttributeValue[AttrMap] = {
    case AttributeValue.Map(map) =>
      Some(new AttrMap(map.toMap.map { case (k, v) => k.value -> v }))
    case _                       => None
  }

  implicit def iterableFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Iterable[A]] = {
    case AttributeValue.List(list) =>
      Some(
        list.map(x =>
          ev.fromAttributeValue(x).get
        ) // this is safe as we should have implicits for all possible AttributeValue types
      )
    case _                         => None
  }

}
