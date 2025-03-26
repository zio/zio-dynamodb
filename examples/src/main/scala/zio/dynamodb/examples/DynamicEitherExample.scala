package zio.dynamodb.examples
import zio._
import zio.json._
import zio.schema.{ DeriveSchema, DynamicValue, Schema }
import zio.schema.annotation.directDynamicMapping

object DynamicEitherExample extends ZIOAppDefault {
//  import zio.schema.codec.json._
  @directDynamicMapping
  case class Person(name: String, age: Int, either: Either[String, Int])

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen

    implicit def dynamicEither[A, B](implicit schemaA: Schema[A], schemaB: Schema[B]): Schema[Either[A, B]] =
      Schema              // we go from ARBITRARY SOURCE SCHEMA -> TARGET SCHEMA[A] VIA A TRANSFORMATION
        .Dynamic()
        .transformOrFail( // DynamicValue => A
          dynamicValue => {
            println(s"XXXXXXXXX dynamicValue: $dynamicValue") // XXXXXXXXX dynamicValue: Primitive(42,int)
            dynamicValue.toTypedValue[A].map(Left.apply).orElse(dynamicValue.toTypedValue[B].map(Right.apply))
          },
          either => { // A => DynamicValue
            println(s"XXXXXXXXX either: $either")
            either match {
              case Left(a)  =>
                Right(DynamicValue.fromSchemaAndValue(schemaA, a))
              case Right(b) =>
                val x: DynamicValue = DynamicValue.fromSchemaAndValue(schemaB, b)
                println(s"XXXXXXXXX b: $x") // Primitive(42,int)
                val y = x.toTypedValue(schemaB)
                println(s"XXXXXXXXX y: $y") // Primitive(42,int)
                Right(DynamicValue.fromSchemaAndValue(schemaB, b))
            }
          }
        )

    implicit val jsonCodec: zio.json.JsonCodec[Person] =
      zio.schema.codec.JsonCodec.jsonCodec(schema)
  }

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    for {
      _             <- ZIO.debug("JSON Codec Example:")
      person: Person = Person("John", 42, Right(42))
      encoded        = person.toJson
      _             <- ZIO.debug(s"person object encoded to JSON string: $encoded")
//      decoded        <- ZIO.fromEither(Person.jsonCodec.decodeJson(encoded))
      // _              <- ZIO.debug(s"JSON object decoded to Person class: $decoded")
      // decoded = """{"name":"John","age":42,"either":{"Int":42}}""".fromJson[Person]
      // _       <- ZIO.debug(s"decoded: $decoded")
    } yield ()
}
