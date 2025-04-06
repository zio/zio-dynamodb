package zio.dynamodb.examples
import zio._
import zio.dynamodb.DynamoDBQuery
import zio.schema.{ DeriveSchema, DynamicValue, Schema, StandardType }
import scala.collection.immutable.ListMap

object DirectDynamicValueFieldExample extends ZIOAppDefault {

  import zio.schema.annotation.directDynamicMapping
  // @directDynamicMapping
  case class Person(id: String, @directDynamicMapping dv: DynamicValue)

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]

    // implicit val jsonCodec: zio.json.JsonCodec[Person] =
    //   zio.schema.codec.JsonCodec.jsonCodec(schema)
  }

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    for {
      _             <- ZIO.debug(s"JSON Codec Example: ${Person.schema}")
      dv             = DynamicValue.Record(
                         id = zio.schema.TypeId.parse("zio.dynamodb.examples.JsonASTFieldExample2.PersonX"),
                         values = ListMap(
                           "name" -> DynamicValue.Primitive[String]("John", StandardType.StringType),
                           "age"  -> DynamicValue.Primitive[Int](42, StandardType.IntType)
                         )
                       )
      person: Person = Person("id", dv)
      encoded        = DynamoDBQuery.toItem(person)
      _             <- ZIO.debug(s"person object encoded: $encoded")
      decoded       <- ZIO.fromEither(DynamoDBQuery.fromItem[Person](encoded))
      _             <- ZIO.debug(s"Item decoded to Person class: $decoded")

    } yield ()
}
