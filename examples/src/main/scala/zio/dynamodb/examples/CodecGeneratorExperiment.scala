package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.{ AttrMap, AttributeValue }
import zio.schema.{ Schema, StandardType }

object CodecGeneratorExperiment extends App {
  type AVEncoder[A]      = A => AttributeValue
  type AttrMapEncoder[A] = A => AttrMap

  final case class SimpleCaseClass2(id: Int, name: String)
  final case class SimpleCaseClass3(id: Int, name: String, flag: Boolean)

  val simpleCaseClass2Schema = Schema.CaseClass2[Int, String, SimpleCaseClass2](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    SimpleCaseClass2,
    _.id,
    _.name
  )

  val simpleCaseClass3Schema = Schema.CaseClass3[Int, String, Boolean, SimpleCaseClass3](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    Schema.Field("flag", Schema[Boolean]),
    SimpleCaseClass3,
    _.id,
    _.name,
    _.flag
  )

  def attrMapEncoder[A](schema: Schema[A]): Option[AttrMapEncoder[A]] =
    schema match {
      case ProductEncoder(encoder) => Some(encoder)
      case _                       => None
    }

  object ProductEncoder {
    def unapply[A](schema: Schema[A]): Option[AttrMapEncoder[A]] =
      schema match {
        case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2)                        =>
          caseClassEncoder(field1 -> extractField1, field2 -> extractField2)
        case Schema.CaseClass3(_, field1, field2, field3, _, extractField1, extractField2, extractField3) =>
          caseClassEncoder(field1 -> extractField1, field2 -> extractField2, field3 -> extractField3)
        case _                                                                                            => None
      }
  }
  def caseClassEncoder[Z](fields: (Schema.Field[_], Z => Any)*): Option[AttrMapEncoder[Z]] =
    Some { (z: Z) =>
      val attrMap: AttrMap = fields.foldRight[AttrMap](AttrMap.empty) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc: Option[AVEncoder[Any]] = schemaEncoder(schema)
          val extractedFieldValue         = ext(z)
          val maybeAttributeValue         = enc.map(_(extractedFieldValue))
          println(s"$key $schema $ext $maybeAttributeValue")

          // TODO: for now ignore errors
          val attrMap = maybeAttributeValue.fold(AttrMap.empty)(av => AttrMap(key -> av))

          acc ++ attrMap
      }

      attrMap
    }

  def schemaEncoder[A](schema: Schema[A]): Option[AVEncoder[A]] =
    schema match {
      case Schema.Primitive(standardType) => primitiveEncoder(standardType)
      case _                              => None
    }

  def primitiveEncoder[A](standardType: StandardType[A]): Option[AVEncoder[A]] =
    standardType match {
      case StandardType.BoolType   => Some((a: A) => AttributeValue.Bool(a.asInstanceOf[Boolean]))
      case StandardType.StringType => Some((a: A) => AttributeValue.String(a.toString))
      case StandardType.ShortType | StandardType.IntType | StandardType.LongType | StandardType.FloatType |
          StandardType.DoubleType =>
        Some((a: A) => AttributeValue.Number(BigDecimal(a.toString)))
      case _                       => None
    }

  val x: Option[AttrMap] = attrMapEncoder(simpleCaseClass2Schema).map(_(SimpleCaseClass2(42, "Avi")))
  println(x)

  val x2: Option[AttrMap] = attrMapEncoder(simpleCaseClass3Schema).map(_(SimpleCaseClass3(42, "Avi", true)))
  println(x2)
}
