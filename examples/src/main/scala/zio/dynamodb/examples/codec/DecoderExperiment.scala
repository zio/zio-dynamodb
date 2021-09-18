//package zio.dynamodb.examples.codec
//
//import zio.dynamodb.examples.codec.Models._
//import zio.dynamodb.{ AttrMap, AttributeValue, FromAttributeValue, Item }
//import zio.schema.{ Schema, StandardType }
//
//import scala.annotation.tailrec
//
//object DecoderExperiment extends App {
//  val nestedItem       = Item("id" -> 1, "name" -> "Avi", "flag" -> true)
//  val parentItem: Item = Item("id" -> 1, "nested" -> nestedItem)
//  val xxx              = for {
//    id     <- parentItem.get[Int]("id") // scalar
//    nested <- parentItem.getItem[SimpleCaseClass3]("nested") { m => // composite case
//                m.as("id", "name", "flag")(SimpleCaseClass3) // recurse until all fields of case class are primitive
//              }
//  } yield NestedCaseClass2(id, nested)
//  println(xxx)
//
//  type Decoder[+A] = AttributeValue => Either[String, A] // can do this other one
//
//  type AttrMapDecoder[A] = AttrMap => Either[String, A]
////  def foo[A](key: String, f: FromAttributeValue[A]): AttrMapDecoder[A] =
////    (am: AttrMap) => {
////      am.get(key)(f)
////    }
//
////  def decoder[A](schema: Schema[A]): Decoder[A] =
//
//  @tailrec
//  def schemaDecoderAttrMap[A](schema: Schema[A], key: String): AttrMapDecoder[A] =
//    schema match {
//      case ProductDecoder(decoder)        => // check AV is an AVMap
//        decoder
//      case Schema.Primitive(standardType) =>
//        (am: AttrMap) => am.get(key)(primitiveDecoder(standardType))
//      // TODO: why do we need this?
//      case l @ Schema.Lazy(_)             =>
//        schemaDecoderAttrMap(l.schema, key)
//      case _                              =>
//        throw new UnsupportedOperationException(s"schema $schema not yet supported")
//    }
//
//  @tailrec
//  def schemaDecoderPrimitive[A](schema: Schema[A]): Either[String, FromAttributeValue[A]] =
//    schema match {
//      case Schema.Primitive(standardType) =>
//        Right(primitiveDecoder(standardType))
//      case l @ Schema.Lazy(_)             =>
//        schemaDecoderPrimitive(l.schema)
//      case _                              =>
//        Left("Boom!")
//    }
//
//  // TODO: get rid of Either in return type when all StandardType's are implemented
//  def primitiveDecoder[A](standardType: StandardType[A]): FromAttributeValue[A] =
//    standardType match {
//      case StandardType.BoolType   => FromAttributeValue[Boolean].asInstanceOf[FromAttributeValue[A]]
//      case StandardType.StringType => FromAttributeValue[String].asInstanceOf[FromAttributeValue[A]]
//      case StandardType.IntType    => FromAttributeValue[Int].asInstanceOf[FromAttributeValue[A]]
//      case _                       => throw new UnsupportedOperationException(s"standardType $standardType not yet supported")
//
//    }
//
//  object ProductDecoder {
//    def unapply[A1, A2, A3, Z](schema: Schema[Z]): Option[AttrMapDecoder[Z]] =
//      schema match {
//        case s @ Schema.CaseClass2(_, _, _, _, _, _)       =>
//          Some(caseClass2Decoder(s))
//        case s @ Schema.CaseClass3(_, _, _, _, _, _, _, _) =>
//          Some(caseClass3Decoder(s))
//        case _                                             =>
//          None
//      }
//  }
//
//  def caseClass2Decoder[A1, A2, Z](schema: Schema.CaseClass2[A1, A2, Z]): AttrMapDecoder[Z] =
//    (am: AttrMap) => {
//      val either: Either[String, Z] = for {
//        from1 <- schemaDecoderAttrMap(schema.field1.schema, "") //  schemaDecoderPrimitive(schema.field1.schema)
//        from2 <- schemaDecoderPrimitive(schema.field2.schema)
//      } yield schema.construct(from1, from2)
//      either.flatten
//    }
//
////  def unsafeDecodeFields(fields: Schema.Field[_]*): List[Any] = ???
//
//  def caseClass3Decoder[A1, A2, A3, Z](schema: Schema.CaseClass3[A1, A2, A3, Z]): AttrMapDecoder[Z] =
//    (am: AttrMap) => {
//      val option: Either[String, Either[String, Z]] = for {
//        from1 <- schemaDecoderPrimitive(schema.field1.schema)
//        from2 <- schemaDecoderPrimitive(schema.field2.schema)
//        from3 <- schemaDecoderPrimitive(schema.field3.schema)
//      } yield am.as(schema.field1.label, schema.field2.label, schema.field3.label)(schema.construct)(
//        from1,
//        from2,
//        from3
//      )
//      option.flatten
//    }
//
////  def extract[A](schema: Schema.Field[A], am: AttrMap)(implicit from: FromAttributeValue[A]): Either[String, A] =
////    schema match {
////      case Schema.Field(key, _, _) =>
////        am.get(key)
////    }
//
//  /*
//  AttrMap refresher
//  ==============================================================================================
//  x = {AttrMap@866} "AttrMap(Map(a -> Number(1), b -> Map(Map(String(b) -> Number(2)))))"
//   map = {Map$Map2@877} "Map$Map2" size = 2
//    0 = {Tuple2@1049} "(a,Number(1))"
//    1 = {Tuple2@1050} "(b,Map(Map(String(b) -> Number(2))))"
//   */
//  val x: Item = Item("a" -> 1, "b" -> Item("b" -> 2))
//  println(x)
//
//  val item = Item("id" -> 1, "name" -> "Avi", "flag" -> true)
//
//  val decoder =
//    schemaDecoderAttrMap(simpleCaseClass3Schema, "parent")
//  val value   = decoder(item)
//  println(value)
//
//}
