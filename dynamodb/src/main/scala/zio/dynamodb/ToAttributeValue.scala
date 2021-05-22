package zio.dynamodb

trait ToAttributeValue[-A] {
  def toAttributeValue(a: A): AttributeValue
}

object ToAttributeValue extends ToAttributeValueLowPriorityImplicits {
  import Predef.{ Map => ScalaMap, String => ScalaString }

  implicit val binaryToAttributeValue: ToAttributeValue[Iterable[Byte]]              = AttributeValue.Binary(_)
  implicit val binarySetToAttributeValue: ToAttributeValue[Iterable[Iterable[Byte]]] = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean]                       = AttributeValue.Bool(_)

  implicit def mapToAttributeValue[A](implicit
    element: ToAttributeValue[A]
  ): ToAttributeValue[ScalaMap[ScalaString, A]] =
    (map: ScalaMap[ScalaString, A]) =>
      AttributeValue.Map {
        map.map {
          case (key, value) => (AttributeValue.String(key), element.toAttributeValue(value))
        }
      }

  implicit val stringToAttributeValue: ToAttributeValue[String]              = AttributeValue.String(_)
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]] =
    AttributeValue.StringSet(_)
  implicit val numberToAttributeValue: ToAttributeValue[BigDecimal]          = AttributeValue.Number(_)
  implicit val numberSetToAttributeValue: ToAttributeValue[Set[BigDecimal]]  = AttributeValue.NumberSet(_)
}
