package zio.dynamodb.examples.codec

import zio.dynamodb.examples.codec.Models._
import zio.dynamodb.{ AttrMap, FromAttributeValue, Item }
import zio.schema.{ Schema, StandardType }

object DecoderExperiment extends App {

  type AttrMapDecoder[A] = AttrMap => Either[String, A]
  def foo[A](key: String, f: FromAttributeValue[A]): AttrMapDecoder[A] =
    (am: AttrMap) => {
      am.get(key)(f)
    }

  def schemaDecoderAttrMap[A](schema: Schema[A], key: String): Option[AttrMapDecoder[A]] =
    schema match {
      case ProductDecoder(decoder)        =>
        Some(decoder)
      case Schema.Primitive(standardType) =>
        primitiveDecoder(standardType).map(f => (am: AttrMap) => am.get(key)(f)).toOption
      case _                              => None
    }

  def schemaDecoderPrimitive[A](schema: Schema[A]): Either[String, FromAttributeValue[A]] =
    schema match {
      case Schema.Primitive(standardType) =>
        primitiveDecoder(standardType)
      case _                              => Left("Boom!")
    }

  // TODO: get rid of Either in return type when all StandardType's are implemented
  def primitiveDecoder[A](standardType: StandardType[A]): Either[String, FromAttributeValue[A]] =
    standardType match {
      case StandardType.BoolType   => Right(FromAttributeValue[Boolean].asInstanceOf[FromAttributeValue[A]])
      case StandardType.StringType => Right(FromAttributeValue[String].asInstanceOf[FromAttributeValue[A]])
      case StandardType.IntType    => Right(FromAttributeValue[Int].asInstanceOf[FromAttributeValue[A]])
      case _                       => Left("Boom!")
    }

  object ProductDecoder {
    def unapply[A1, A2, A3, Z](schema: Schema[Z]): Option[AttrMapDecoder[Z]] =
      schema match {
        case s @ Schema.CaseClass2(_, _, _, _, _, _)       =>
          Some(caseClass2Decoder(s))
        case s @ Schema.CaseClass3(_, _, _, _, _, _, _, _) =>
          Some(caseClass3Decoder(s))
        case _                                             =>
          None
      }
  }

  def caseClass2Decoder[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z]): AttrMapDecoder[Z] =
    (am: AttrMap) => {
      val option: Either[String, Either[String, Z]] = for {
        from1 <- schemaDecoderPrimitive(schema.field1.schema)
        from2 <- schemaDecoderPrimitive(schema.field2.schema)
      } yield am.as(schema.field1.label, schema.field2.label)(schema.construct)(from1, from2)
      option.flatten
    }

  def caseClass3Decoder[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z]): AttrMapDecoder[Z] =
    (am: AttrMap) => {
      val option: Either[String, Either[String, Z]] = for {
        from1 <- schemaDecoderPrimitive(schema.field1.schema)
        from2 <- schemaDecoderPrimitive(schema.field2.schema)
        from3 <- schemaDecoderPrimitive(schema.field3.schema)
      } yield am.as(schema.field1.label, schema.field2.label, schema.field3.label)(schema.construct)(
        from1,
        from2,
        from3
      )
      option.flatten
    }

  def extract[A](schema: Schema.Field[A], am: AttrMap)(implicit from: FromAttributeValue[A]): Either[String, A] =
    schema match {
      case Schema.Field(key, _, _) =>
        am.get(key)
    }

  /*
  AttrMap refresher
  ==============================================================================================
  x = {AttrMap@866} "AttrMap(Map(a -> Number(1), b -> Map(Map(String(b) -> Number(2)))))"
   map = {Map$Map2@877} "Map$Map2" size = 2
    0 = {Tuple2@1049} "(a,Number(1))"
    1 = {Tuple2@1050} "(b,Map(Map(String(b) -> Number(2))))"
   */
  val x: Item = Item("a" -> 1, "b" -> Item("b" -> 2))
  println(x)

  val item = Item("id" -> 1, "name" -> "Avi", "flag" -> true)

  val decoder: Option[AttrMapDecoder[SimpleCaseClass3]] =
    schemaDecoderAttrMap(simpleCaseClass3Schema, "parent")
  val value                                             = decoder.map(_(item))
  println(value)
}
