package zio.dynamodb.examples.codec

import zio.Chunk
import zio.dynamodb.{ AttrMap, AttributeValue, ToAttributeValue }
import zio.schema.{ Schema, StandardType }

final case class AttrMapEncoder[A](encode: A => AttrMap) extends Function[A, AttrMap] { self =>

  def apply(a: A): AttrMap = encode(a)
}
object AttrMapEncoder {
  def fromAvEncoder[A](key: String, f: ToAttributeValue[A]): AttrMapEncoder[A] =
    AttrMapEncoder(encode = (a: A) => AttrMap(key -> f.toAttributeValue(a)))
}

object EncoderExperiment extends App {
//  type AttrMapEncoder[A] = A => AttrMap
//  def toAvEncoder[A](f: AVEncoder[A], key: String): AttrMapEncoder[A] = (a: A) => AttrMap(key -> f(a))

  final case class NestedCaseClass2(id: Int, nested: SimpleCaseClass3)
  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)

  lazy implicit val simpleCaseClass3Schema = Schema.CaseClass3[Int, String, Boolean, SimpleCaseClass3](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    Schema.Field("flag", Schema[Boolean]),
    SimpleCaseClass3,
    _.id,
    _.name,
    _.flag
  )

  val simpleCaseClass2Schema = Schema.CaseClass2[Int, SimpleCaseClass3, NestedCaseClass2](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("nested", Schema[SimpleCaseClass3]),
    NestedCaseClass2,
    _.id,
    _.nested
  )

  // TODO: remove Option on return type when all encodings are implemented
  def schemaEncoder[A](schema: Schema[A], key: String): Option[AttrMapEncoder[A]] =
    schema match {
      case ProductEncoder(encoder)        =>
        Some(encoder)
      case Schema.Primitive(standardType) =>
        primitiveEncoder(standardType).map(AttrMapEncoder.fromAvEncoder(key, _))
      case _                              =>
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
          println(s"$key $schema $ext $maybeAttrMap")

          // TODO: for now ignore errors
          val attrMap = maybeAttrMap.getOrElse(AttrMap.empty)

          schema match {
            case ProductEncoder(_) =>
              acc ++ AttrMap(key -> attrMap)
            case _                 =>
              acc ++ attrMap
          }
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

  val x2: Option[AttrMap] = schemaEncoder(simpleCaseClass3Schema, "parent").map(_(SimpleCaseClass3(42, "Avi", true)))
  println(x2)

  val x: Option[AttrMap] =
    schemaEncoder(simpleCaseClass2Schema, "parent").map(_(NestedCaseClass2(42, SimpleCaseClass3(1, "Avi", true))))
  println(x)

}
