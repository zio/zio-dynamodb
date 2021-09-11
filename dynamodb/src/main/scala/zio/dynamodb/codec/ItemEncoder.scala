package zio.dynamodb.codec

import zio.dynamodb.{ AttrMap, AttributeValue, Item, ToAttributeValue }
import zio.schema.{ Schema, StandardType }

import scala.annotation.tailrec

final case class AttrMapEncoder[A](encode: A => AttrMap) extends Function[A, AttrMap] { self =>

  def apply(a: A): AttrMap = encode(a)
}
object AttrMapEncoder {
  def fromAvEncoder[A](key: String, f: ToAttributeValue[A]): AttrMapEncoder[A] =
    AttrMapEncoder(encode = (a: A) => AttrMap(key -> f.toAttributeValue(a)))
}

object ItemEncoder {
  def toItem[A](a: A)(implicit schema: Schema[A]): Item = unsafeSchemaEncoder(schema, a)

  // temp function until all attribute mappings are done
  def unsafeSchemaEncoder[A](schema: Schema[A], a: A): Item =
    schemaEncoder(schema, "top_level_key_ignored").getOrElse(throw new Exception(s"problems encoding $a"))(a)

  // TODO: remove Option on return type when all encodings are implemented
  @tailrec
  def schemaEncoder[A](schema: Schema[A], key: String): Option[AttrMapEncoder[A]] =
    schema match {
      case ProductEncoder(encoder)        =>
        Some(encoder)
      case Schema.Primitive(standardType) =>
        primitiveEncoder(standardType).map(AttrMapEncoder.fromAvEncoder(key, _))

      // TODO: why do we need this?
      case l @ Schema.Lazy(_)             => schemaEncoder(l.schema, key)

      case _ =>
        None
    }

  object ProductEncoder {
    def unapply[A](schema: Schema[A]): Option[AttrMapEncoder[A]] =
      schema match {
        case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2)                        =>
          caseClassEncoder(field1 -> extractField1, field2 -> extractField2)
        case Schema.CaseClass3(_, field1, field2, field3, _, extractField1, extractField2, extractField3) =>
          caseClassEncoder(field1 -> extractField1, field2 -> extractField2, field3 -> extractField3)
        case _                                                                                            =>
          None
      }
  }

  def caseClassEncoder[Z](fields: (Schema.Field[_], Z => Any)*): Option[AttrMapEncoder[Z]] =
    Some(AttrMapEncoder { (z: Z) =>
      val attrMap: AttrMap = fields.foldRight[AttrMap](AttrMap.empty) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc: Option[AttrMapEncoder[Any]] = schemaEncoder(schema, key)
          val extractedFieldValue              = ext(z)
          val maybeAttrMap: Option[AttrMap]    = enc.map(_(extractedFieldValue))

          // TODO: for now ignore errors
          val attrMap = maybeAttrMap.getOrElse(AttrMap.empty)

          @tailrec
          def combineAttrMaps[A](schema: Schema[A]): AttrMap =
            schema match {
              case l @ Schema.Lazy(_) =>
                combineAttrMaps(l.schema)
              case ProductEncoder(_)  =>
                acc ++ AttrMap(key -> attrMap)
              case _                  =>
                acc ++ attrMap
            }

          combineAttrMaps(schema)
      }

      attrMap
    })

  def primitiveEncoder[A](standardType: StandardType[A]): Option[ToAttributeValue[A]] =
    standardType match {
      case StandardType.BoolType   =>
        Some((a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean])) // TODO: try to use ToAttributeValue machinery
      case StandardType.StringType => Some((a: A) => AttributeValue.String(a.toString))
      case StandardType.ShortType | StandardType.IntType | StandardType.LongType | StandardType.FloatType |
          StandardType.DoubleType =>
        Some((a: A) => AttributeValue.Number(BigDecimal(a.toString)))
      case _                       => None
    }

}
