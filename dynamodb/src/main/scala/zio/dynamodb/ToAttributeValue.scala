package zio.dynamodb

import zio.Chunk

trait ToAttributeValue[A] {
  def toAttributeValue(a: A): AttributeValue
}

object ToAttributeValue extends ToAttributeValueLowPriorityImplicits0 {
  import Predef.{ String => ScalaString }

  implicit def binaryToAttributeValue[Col[A] <: Iterable[A]]: ToAttributeValue[Col[Byte]] = AttributeValue.Binary(_)
  implicit def binarySetToAttributeValue[Col1[A] <: Iterable[A], Col2[B] <: Iterable[B]]
    : ToAttributeValue[Col1[Col2[Byte]]]                                                  = AttributeValue.BinarySet(_)
  implicit val boolToAttributeValue: ToAttributeValue[Boolean]                            = AttributeValue.Bool(_)

  implicit val attrMapToAttributeValue: ToAttributeValue[AttrMap] =
    (attrMap: AttrMap) =>
      AttributeValue.Map {
        attrMap.map.map {
          case (key, value) => (AttributeValue.String(key), value)
        }
      }

  implicit def mapToAttributeValue[A](implicit ev: ToAttributeValue[A]): ToAttributeValue[Map[String, A]] =
    (map: Map[String, A]) =>
      AttributeValue.Map(map.map { case (k, v) => AttributeValue.String(k) -> ev.toAttributeValue(v) })

  implicit val stringToAttributeValue: ToAttributeValue[String]                                           = AttributeValue.String(_)
  implicit val stringSetToAttributeValue: ToAttributeValue[Set[ScalaString]]                              =
    AttributeValue.StringSet(_)
  // BigDecimal support
  implicit val bigDecimalToAttributeValue: ToAttributeValue[BigDecimal]                                   = AttributeValue.Number(_)
  implicit val bigDecimalSetToAttributeValue: ToAttributeValue[Set[BigDecimal]]                           = AttributeValue.NumberSet(_)
  // Int support
  implicit val intToAttributeValue: ToAttributeValue[Int]                                                 = (a: Int) => AttributeValue.Number(BigDecimal(a))
  implicit val intSetToAttributeValue: ToAttributeValue[Set[Int]]                                         = (a: Set[Int]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Long support
  implicit val longToAttributeValue: ToAttributeValue[Long]                                               = (a: Long) => AttributeValue.Number(BigDecimal(a))
  implicit val longSetToAttributeValue: ToAttributeValue[Set[Long]]                                       = (a: Set[Long]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Double support
  implicit val doubleToAttributeValue: ToAttributeValue[Double]                                           = (a: Double) => AttributeValue.Number(BigDecimal(a))
  implicit val doubleSetToAttributeValue: ToAttributeValue[Set[Double]]                                   = (a: Set[Double]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.apply))
  // Float support
  implicit val floatToAttributeValue: ToAttributeValue[Float]                                             = (a: Float) =>
    AttributeValue.Number(BigDecimal.decimal(a))
  implicit val floatSetToAttributeValue: ToAttributeValue[Set[Float]]                                     = (a: Set[Float]) =>
    AttributeValue.NumberSet(a.map(BigDecimal.decimal))

}

trait ToAttributeValueLowPriorityImplicits0 extends ToAttributeValueLowPriorityImplicits1 {
  implicit def collectionToAttributeValue[Col[X] <: Iterable[X], A](implicit
    element: ToAttributeValue[A]
  ): ToAttributeValue[Col[A]] =
    (xs: Col[A]) => AttributeValue.List(Chunk.fromIterable(xs.map(element.toAttributeValue)))

}

trait ToAttributeValueLowPriorityImplicits1 {
  implicit val nullToAttributeValue: ToAttributeValue[Null] = (_: Null) => AttributeValue.Null
}
