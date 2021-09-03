package zio.dynamodb.examples

import zio.Chunk
import zio.dynamodb.{ AttrMap, AttributeValue }
import zio.schema.{ Schema, StandardType }

object CodecGeneratorExperiment extends App {
  type Encoder[A] = A => AttributeValue

  final case class SimpleCaseClass(id: Int, name: String)

  val simpleCaseClassSchema = Schema.CaseClass2[Int, String, SimpleCaseClass](
    Chunk.empty,
    Schema.Field("id", Schema[Int]),
    Schema.Field("name", Schema[String]),
    SimpleCaseClass(_, _),
    _.id,
    _.name
  )

  def encoder[A](schema: Schema[A]): Option[Encoder[A]] =
    schema match {
      case Schema.CaseClass2(_, field1, field2, _, extractField1, extractField2) =>
        caseClassEncoder(field1 -> extractField1, field2 -> extractField2)
      case _                                                                     => None
    }

  def caseClassEncoder[B](fields: (Schema.Field[_], B => Any)*): Option[Encoder[B]] =
    Some { (b: B) =>
      println(s"$fields $b")
      fields.foreach {
        case (Schema.Field(key, schema, _), ext) =>
          val enc: Option[Encoder[Any]] = schemaEncoder(schema)
          val extractedFieldValue       = ext(b)
          val maybeAttributeValue       = enc.map(_(extractedFieldValue))
          println(s"$key $schema $ext $maybeAttributeValue, ")
      }
      AttributeValue.Null
    }

  def schemaEncoder[A](schema: Schema[A]): Option[Encoder[A]] =
    schema match {
      case Schema.Primitive(standardType) => primitiveEncoder(standardType)
      case _                              => None
    }

  def primitiveEncoder[A](standardType: StandardType[A]): Option[Encoder[A]] =
    standardType match {
      case StandardType.StringType => Some((a: A) => AttributeValue.String(a.toString))
      case StandardType.IntType    => Some((a: A) => AttributeValue.Number(BigDecimal(a.toString)))
      case _                       => None
    }

  val x: Option[AttributeValue] = encoder(simpleCaseClassSchema).map(_(SimpleCaseClass(42, "Avi")))
  println(x)
}
