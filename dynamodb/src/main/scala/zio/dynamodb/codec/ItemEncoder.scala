package zio.dynamodb.codec

import zio.dynamodb.{ AttributeValue, FromAttributeValue, Item }
import zio.schema.{ Schema, StandardType }

import scala.annotation.tailrec

object ItemEncoder {
  type Encoder[A] = A => AttributeValue

  def toItem[A](a: A)(implicit schema: Schema[A]): Item =
    FromAttributeValue.attrMapFromAttributeValue
      .fromAttributeValue(encoder(schema)(a))
      .getOrElse(throw new Exception(s"error encoding $a"))

  @tailrec
  def encoder[A](schema: Schema[A]): Encoder[A] =
    schema match {
      case ProductEncoder(encoder)        =>
        encoder
      case Schema.Primitive(standardType) =>
        primitiveEncoder(standardType)
      case l @ Schema.Lazy(_)             => encoder(l.schema)
      case _                              =>
        throw new UnsupportedOperationException(s"schema $schema not yet supported")
    }

  object ProductEncoder {
    def unapply[A](schema: Schema[A]): Option[Encoder[A]] =
      schema match {
        case Schema.CaseClass1(_, field1, _, extractField1)                                               =>
          Some(caseClassEncoder(field1 -> extractField1))
        case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2)                        =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2))
        case Schema.CaseClass3(_, field1, field2, field3, _, extractField1, extractField2, extractField3) =>
          Some(caseClassEncoder(field1 -> extractField1, field2 -> extractField2, field3 -> extractField3))
        case _                                                                                            =>
          None
      }
  }

  def caseClassEncoder[A](fields: (Schema.Field[_], A => Any)*): Encoder[A] =
    (a: A) => {
      val avMap: AttributeValue.Map = fields.foldRight[AttributeValue.Map](AttributeValue.Map(Map.empty)) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc                 = encoder(schema)
          val extractedFieldValue = ext(a)
          val av                  = enc(extractedFieldValue)

          @tailrec
          def appendToMap[B](schema: Schema[B]): AttributeValue.Map =
            schema match {
              case l @ Schema.Lazy(_) =>
                appendToMap(l.schema)
              case _                  =>
                AttributeValue.Map(acc.value + (AttributeValue.String(key) -> av))
            }

          appendToMap(schema)
      }
      avMap
    }

  def primitiveEncoder[A](standardType: StandardType[A]): Encoder[A] = { (a: A) =>
    standardType match {
      case StandardType.BoolType   =>
        AttributeValue.Bool(a.asInstanceOf[Boolean])
      case StandardType.StringType => AttributeValue.String(a.toString)
      case StandardType.ShortType | StandardType.IntType | StandardType.LongType | StandardType.FloatType |
          StandardType.DoubleType =>
        AttributeValue.Number(BigDecimal(a.toString))
      case _                       =>
        throw new UnsupportedOperationException(s"StandardType $standardType not yet supported")
    }
  }

}
