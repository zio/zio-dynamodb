package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.{ AttrMap, AttributeValue }
import zio.schema.{ Schema, StandardType }

object CodecGeneratorExperiment extends App {
  type AVEncoder[A]      = A => AttributeValue
  type AttrMapEncoder[A] = A => AttrMap

  final case class SimpleCaseClass(id: Int, name: String)

  val simpleCaseClassSchema = Schema.CaseClass2[Int, String, SimpleCaseClass](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    SimpleCaseClass,
    _.id,
    _.name
  )

  def attrMapEncoder[A](schema: Schema[A]): Option[AttrMapEncoder[A]] =
    schema match {
      case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2) =>
        caseClassEncoder(field1 -> extractField1, field2 -> extractField2)
      case _                                                                     => None
    }

  def caseClassEncoder[B](fields: (Schema.Field[_], B => Any)*): Option[AttrMapEncoder[B]] =
    Some { (b: B) =>
      val attrMap: AttrMap = fields.foldRight[AttrMap](AttrMap.empty) {
        case ((Schema.Field(key, schema, _), ext), acc) =>
          val enc: Option[AVEncoder[Any]] = schemaEncoder(schema)
          val extractedFieldValue         = ext(b)
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
      case StandardType.StringType => Some((a: A) => AttributeValue.String(a.toString))
      case StandardType.IntType    => Some((a: A) => AttributeValue.Number(BigDecimal(a.toString)))
      case _                       => None
    }

  val x: Option[AttrMap] = attrMapEncoder(simpleCaseClassSchema).map(_(SimpleCaseClass(42, "Avi")))
  println(x)
}
