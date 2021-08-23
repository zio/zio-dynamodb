package zio.dynamodb

trait FromAttributeValue[+A] {
  // TODO: is a partial function the best way to capture this?
  def fromAttributeValue(av: AttributeValue): Option[A]
}

object FromAttributeValue {

  def apply[A](implicit from: FromAttributeValue[A]): FromAttributeValue[A] = from

  /*
  private[dynamodb] final case class BinarySet(value: Iterable[Iterable[Byte]])   extends AttributeValue
  private[dynamodb] final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  private[dynamodb] case object Null                                              extends AttributeValue
  private[dynamodb] final case class List(value: Iterable[AttributeValue])        extends AttributeValue

  private[dynamodb] final case class Binary(value: Iterable[Byte])                extends AttributeValue
  private[dynamodb] final case class Bool(value: Boolean)                         extends AttributeValue
  private[dynamodb] final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  private[dynamodb] final case class Number(value: BigDecimal)                    extends AttributeValue
  private[dynamodb] final case class String(value: ScalaString)                   extends AttributeValue
  private[dynamodb] final case class StringSet(value: Set[ScalaString])           extends AttributeValue
   */

  implicit def optionFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Option[A]] = {
    case AttributeValue.Null =>
      Some(None)
    case av: AttributeValue  =>
      val a = ev.fromAttributeValue(av)
      Option(a)
  }

  implicit val booleanDecimalFromAttributeValue: FromAttributeValue[Boolean] = {
    case AttributeValue.Bool(b) => Some(b)
    case _                      => None
  }

  implicit val stringFromAttributeValue: FromAttributeValue[String] = {
    case AttributeValue.String(s) => Some(s)
    case _                        => None
  }

  implicit val intFromAttributeValue: FromAttributeValue[Int]                       = {
    case AttributeValue.Number(bd) => Some(bd.intValue)
    case _                         => None
  }
  implicit val intSetFromAttributeValue: FromAttributeValue[Set[Int]]               = {
    case AttributeValue.NumberSet(bdSet) => Some(bdSet.map(_.intValue))
    case _                               => None
  }
  implicit val shortFromAttributeValue: FromAttributeValue[Short]                   = {
    case AttributeValue.Number(bd) => Some(bd.shortValue)
    case _                         => None
  }
  implicit val shortSetFromAttributeValue: FromAttributeValue[Set[Short]]           = {
    case AttributeValue.NumberSet(bdSet) => Some(bdSet.map(_.shortValue))
    case _                               => None
  }
  implicit val floatFromAttributeValue: FromAttributeValue[Float]                   = {
    case AttributeValue.Number(bd) => Some(bd.floatValue)
    case _                         => None
  }
  implicit val floatSetFromAttributeValue: FromAttributeValue[Set[Float]]           = {
    case AttributeValue.NumberSet(bdSet) => Some(bdSet.map(_.floatValue))
    case _                               => None
  }
  implicit val doubleFromAttributeValue: FromAttributeValue[Double]                 = {
    case AttributeValue.Number(bd) => Some(bd.doubleValue)
    case _                         => None
  }
  implicit val doubleSetFromAttributeValue: FromAttributeValue[Set[Double]]         = {
    case AttributeValue.NumberSet(bdSet) => Some(bdSet.map(_.doubleValue))
    case _                               => None
  }
  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal]         = {
    case AttributeValue.Number(bd) => Some(bd)
    case _                         => None
  }
  implicit val bigDecimalSetFromAttributeValue: FromAttributeValue[Set[BigDecimal]] = {
    case AttributeValue.NumberSet(bdSet) => Some(bdSet.map(_.bigDecimal))
    case _                               => None
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
