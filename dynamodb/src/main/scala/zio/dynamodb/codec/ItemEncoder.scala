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
  def toItem[A](a: A)(implicit schema: Schema[A]): Item = unsafeSchemaEncoder(schema, "top_level_key_ignored")(a)

  // TODO: remove unsafe prefix when all encodings are implemented
  @tailrec
  def unsafeSchemaEncoder[A](schema: Schema[A], key: String): AttrMapEncoder[A] =
    schema match {
      case ProductEncoder(encoder)        =>
        encoder
      case Schema.Primitive(standardType) =>
        AttrMapEncoder.fromAvEncoder(key, primitiveEncoder(standardType))

      // TODO: why do we need this?
      case l @ Schema.Lazy(_)             => unsafeSchemaEncoder(l.schema, key)

      case _ =>
        throw new UnsupportedOperationException(s"schema $schema not yet supported")
    }

  object ProductEncoder {
    def unapply[A](schema: Schema[A]): Option[AttrMapEncoder[A]] =
      schema match {
        case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2)                        =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2))
        case Schema.CaseClass3(_, field1, field2, field3, _, extractField1, extractField2, extractField3) =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2, field3 -> extractField3))
        case _                                                                                            =>
          None
      }
  }

  def caseClassEncoder[Z](fields: (Schema.Field[_], Z => Any)*): AttrMapEncoder[Z] =
    AttrMapEncoder { (z: Z) =>
      val attrMap: AttrMap = fields.foldRight[AttrMap](AttrMap.empty) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc: AttrMapEncoder[Any] = unsafeSchemaEncoder(schema, key)
          val extractedFieldValue      = ext(z)
          val attrMap: AttrMap         = enc(extractedFieldValue)

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
    }

  def primitiveEncoder[A](standardType: StandardType[A]): ToAttributeValue[A] =
    standardType match {
      case StandardType.BoolType   =>
        (a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean]) // TODO: try to use ToAttributeValue machinery
      case StandardType.StringType => (a: A) => AttributeValue.String(a.toString)
      case StandardType.ShortType | StandardType.IntType | StandardType.LongType | StandardType.FloatType |
          StandardType.DoubleType =>
        (a: A) => AttributeValue.Number(BigDecimal(a.toString))
      case _                       =>
        throw new UnsupportedOperationException(s"StandardType $standardType not yet supported")
    }

}
