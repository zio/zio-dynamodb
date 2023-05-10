package zio.dynamodb

import zio.Chunk
import zio.dynamodb.DynamoDBQuery.put
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{ assert, assertTrue, ZIOSpecDefault }

object ZStreamPipeliningSpec extends ZIOSpecDefault {
  final case class Person(id: Int, name: String)

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]
  }

  private val people       = (1 to 200).map(i => Person(i, s"name$i")).toList
  private val personStream = ZStream.fromIterable(people)

  override def spec =
    suite("ZStream piplelining suite")(
      test("round trip test") {
        for {
          _            <- TestDynamoDBExecutor.addTable("person", "id")
          _            <- batchWriteFromStream(personStream) { person =>
                            put("person", person)
                          }.runDrain
          actualPeople <- batchReadFromStream[Any, Person, Person]("person", personStream)(person =>
                            PrimaryKey("id" -> person.id)
                          ).right.runCollect
        } yield assert(actualPeople.toList.map(_._2))(equalTo(people))
      },
      test("surfaces successfully found items as Right elements and errors as Left elements in the ZStream") {
        for {
          _            <- TestDynamoDBExecutor.addTable(
                            "person",
                            "id",
                            PrimaryKey("id" -> 1) -> Item("id" -> 1, "name" -> "Avi"),
                            PrimaryKey("id" -> 2) -> Item("id" -> 2, "boom!" -> "de-serialisation-error-expected")
                          )
          actualPeople <- batchReadFromStream[Any, Person, Person]("person", personStream.take(3))(person =>
                            PrimaryKey("id" -> person.id)
                          ).runCollect
        } yield assertTrue(
          actualPeople.map(_.map(_._2)) == Chunk(
            Right(Person(1, "Avi")),
            Left(
              DynamoDBError.DecodingError(message =
                "field 'name' not found in Map(Map(String(id) -> Number(2), String(boom!) -> String(de-serialisation-error-expected)))"
              )
            ),
            Left(value =
              DynamoDBError.ValueNotFound(message = "value with key AttrMap(Map(id -> Number(3))) not found")
            )
          )
        )
      }
    ).provide(DynamoDBExecutor.test)
}
