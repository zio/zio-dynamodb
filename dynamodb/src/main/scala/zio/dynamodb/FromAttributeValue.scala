package zio.dynamodb

trait FromAttributeValue[+A] {
  def fromAttributeValue(av: AttributeValue): Either[String, A]
}

object FromAttributeValue {

  def apply[A](implicit from: FromAttributeValue[A]): FromAttributeValue[A] = from

  implicit def optionFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Option[A]] = {
    case AttributeValue.Null =>
      Right(None)
    case av: AttributeValue  =>
      ev.fromAttributeValue(av).map(Some(_))
  }

  implicit val binaryFromAttributeValue: FromAttributeValue[Iterable[Byte]] = {
    case AttributeValue.Binary(b) => Right(b)
    case av                       => Left(s"Error getting binary value. Expected AttributeValue.Binary but found $av")
  }

  implicit def binarySetFromAttributeValue: FromAttributeValue[Iterable[Iterable[Byte]]] = {
    case AttributeValue.BinarySet(set) => Right(set)
    case av                            => Left(s"Error getting binary set value. Expected AttributeValue.BinarySet but found $av")
  }

  implicit val booleanFromAttributeValue: FromAttributeValue[Boolean] = {
    case AttributeValue.Bool(b) => Right(b)
    case av                     => Left(s"Error getting boolean value. Expected AttributeValue.Bool but found $av")
  }

  implicit val stringFromAttributeValue: FromAttributeValue[String] = {
    case AttributeValue.String(s) => Right(s)
    case av                       => Left(s"Error getting string value. Expected AttributeValue.String but found $av")
  }

  implicit val shortFromAttributeValue: FromAttributeValue[Short]                   = {
    case AttributeValue.Number(bd) => Right(bd.shortValue)
    case av                        => Left(s"Error getting short value. Expected AttributeValue.Number but found $av")
  }
  implicit val shortSetFromAttributeValue: FromAttributeValue[Set[Short]]           = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.shortValue))
    case av                              => Left(s"Error getting short set value. Expected AttributeValue.NumberSet but found $av")
  }
  implicit val intFromAttributeValue: FromAttributeValue[Int]                       = {
    case AttributeValue.Number(bd) => Right(bd.intValue)
    case av                        => Left(s"Error getting int value. Expected AttributeValue.Number but found $av")
  }
  implicit val intSetFromAttributeValue: FromAttributeValue[Set[Int]]               = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.intValue))
    case av                              => Left(s"Error getting int set value. Expected AttributeValue.NumberSet but found $av")
  }
  implicit val longFromAttributeValue: FromAttributeValue[Long]                     = {
    case AttributeValue.Number(bd) => Right(bd.longValue)
    case av                        => Left(s"Error getting long value. Expected AttributeValue.Number but found $av")
  }
  implicit val longSetFromAttributeValue: FromAttributeValue[Set[Long]]             = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.longValue))
    case av                              => Left(s"Error getting long set value. Expected AttributeValue.Number but found $av")
  }
  implicit val floatFromAttributeValue: FromAttributeValue[Float]                   = {
    case AttributeValue.Number(bd) => Right(bd.floatValue)
    case av                        => Left(s"Error getting float value. Expected AttributeValue.Number but found $av")
  }
  implicit val floatSetFromAttributeValue: FromAttributeValue[Set[Float]]           = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.floatValue))
    case av                              => Left(s"Error getting float set value. Expected AttributeValue.Number but found $av")
  }
  implicit val doubleFromAttributeValue: FromAttributeValue[Double]                 = {
    case AttributeValue.Number(bd) => Right(bd.doubleValue)
    case av                        => Left(s"Error getting double value. Expected AttributeValue.Number but found $av")
  }
  implicit val doubleSetFromAttributeValue: FromAttributeValue[Set[Double]]         = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet.map(_.doubleValue))
    case av                              => Left(s"Error getting double value. Expected AttributeValue.Number but found $av")
  }
  implicit val bigDecimalFromAttributeValue: FromAttributeValue[BigDecimal]         = {
    case AttributeValue.Number(bd) => Right(bd)
    case av                        => Left(s"Error getting BigDecimal value. Expected AttributeValue.Number but found $av")
  }
  implicit val bigDecimalSetFromAttributeValue: FromAttributeValue[Set[BigDecimal]] = {
    case AttributeValue.NumberSet(bdSet) => Right(bdSet)
    case av                              => Left(s"Error getting BigDecimal set value. Expected AttributeValue.Number but found $av")
  }

  implicit def mapFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Map[String, A]] = {
    case AttributeValue.Map(map) =>
      EitherUtil
        .forEach(map.toMap.toList) {
          case (avK, avV) =>
            ev.fromAttributeValue(avV).map(v => (avK.value, v))
        }
        .map(_.toMap)
    case av                      => Left(s"Error getting map value. Expected AttributeValue.Map but found $av")
  }

  implicit def stringSetFromAttributeValue: FromAttributeValue[Set[String]] = {
    case AttributeValue.StringSet(set) => Right(set)
    case av                            => Left(s"Error getting string set value. Expected AttributeValue.StringSet but found $av")
  }

  implicit val attrMapFromAttributeValue: FromAttributeValue[AttrMap] = {
    case AttributeValue.Map(map) =>
      Right(new AttrMap(map.toMap.map { case (k, v) => k.value -> v }))
    case av                      => Left(s"Error getting AttrMap value. Expected AttributeValue.Map but found $av")
  }

  implicit def iterableFromAttributeValue[A](implicit ev: FromAttributeValue[A]): FromAttributeValue[Iterable[A]] = {
    case AttributeValue.List(list) =>
      EitherUtil.forEach(list)(ev.fromAttributeValue)
    case av                        => Left(s"Error getting iterable value. Expected AttributeValue.List but found $av")
  }

}
