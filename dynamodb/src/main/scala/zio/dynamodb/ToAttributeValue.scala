package zio.dynamodb

trait ToAttributeValue[-A] {
  def toAttributeValue(a: A): AttributeValue
}

object ToAttributeValue extends ToAttributeValueLowPriorityImplicits {
  import Predef.{ String => ScalaString }

  implicit val binaryToAttributeValue: ToAttributeValue[Iterable[Byte]]              = AttributeValue.Binary(_)
  implicit val binarySetToAttributeValue: ToAttributeValue[Iterable[Iterable[Byte]]] = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean]                       = AttributeValue.Bool(_)

  implicit val attrMapToAttributeValue: ToAttributeValue[AttrMap] =
    (attrMap: AttrMap) =>
      AttributeValue.Map {
        attrMap.map.map {
          case (key, value) => (AttributeValue.String(key), value)
        }
      }

  implicit val nullToAttributeValue: ToAttributeValue[AttributeValue.Null.type] = _ => AttributeValue.Null

  implicit val stringToAttributeValue: ToAttributeValue[String]                 = AttributeValue.String(_)
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]]    =
    AttributeValue.StringSet(_)
  // BigDecimal support
  implicit val bigDecimalToAttributeValue: ToAttributeValue[BigDecimal]         = AttributeValue.Number(_)
  implicit val bigDecimalSetToAttributeValue: ToAttributeValue[Set[BigDecimal]] = AttributeValue.NumberSet(_)
  // Int support
  implicit val intToAttributeValue: ToAttributeValue[Int]                       = (a: Int) => AttributeValue.Number(BigDecimal(a))
  implicit val intSetToAttributeValue: ToAttributeValue[Set[Int]]               = (a: Set[Int]) =>
    AttributeValue.NumberSet(a.map(BigDecimal(_)))
  // Long support
  implicit val longToAttributeValue: ToAttributeValue[Long]                     = (a: Long) => AttributeValue.Number(BigDecimal(a))
  implicit val longSetToAttributeValue: ToAttributeValue[Set[Long]]             = (a: Set[Long]) =>
    AttributeValue.NumberSet(a.map(BigDecimal(_)))
  // Double support
  implicit val doubleToAttributeValue: ToAttributeValue[Double]                 = (a: Double) => AttributeValue.Number(BigDecimal(a))
  implicit val doubleSetToAttributeValue: ToAttributeValue[Set[Double]]         = (a: Set[Double]) =>
    AttributeValue.NumberSet(a.map(BigDecimal(_)))
  // Float support
  implicit val floatToAttributeValue: ToAttributeValue[Float]                   = (a: Float) =>
    AttributeValue.Number(BigDecimal.decimal(a))
  implicit val floatSetToAttributeValue: ToAttributeValue[Set[Float]]           = (a: Set[Float]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.decimal))

}
