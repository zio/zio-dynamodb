package zio.dynamodb.examples
import zio._
import zio.json._
import zio.schema.{ DeriveSchema, DynamicValue, Schema }
import zio.schema.annotation.directDynamicMapping

object DynamicEitherExample extends ZIOAppDefault {
  
  case class Person(name: String, age: Int, @directDynamicMapping either: Either[String, Int])

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen

    implicit def dynamicEither[A, B](implicit schemaA: Schema[A], schemaB: Schema[B]): Schema[Either[A, B]] =
      Schema
        .Dynamic()
        .transformOrFail(
          dynamicValue =>
            dynamicValue.toTypedValue[A].map(Left.apply).orElse(dynamicValue.toTypedValue[B].map(Right.apply)),
          either =>
            either match {
              case Left(a)  =>
                Right(DynamicValue.fromSchemaAndValue(schemaA, a))
              case Right(b) =>
                Right(DynamicValue.fromSchemaAndValue(schemaB, b))
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
      // person object encoded to JSON string: {"name":"John","age":42,"either":{"Int":42}}
    } yield ()
}
