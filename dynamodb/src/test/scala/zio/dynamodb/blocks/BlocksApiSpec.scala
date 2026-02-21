package zio.dynamodb.blocks

import zio.ZIO
import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.blocks.BlocksApi._
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery }
import zio.test.Assertion.{ containsString, fails, hasMessage, isSubtype }
import zio.test.{ assert, assertTrue, TestResult, ZIOSpecDefault }

object BlocksApiSpec extends ZIOSpecDefault {
  final case class Person(id: String, age: Int, list: List[String] = Nil)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String]                 = $(_.id)
    val age: Lens[Person, Int]                   = $(_.age)
    val list: Lens[Person, List[String]]         = $(_.list)
    def atList(i: Int): Optional[Person, String] = $(_.list.at(i))
  }
  val personTable = "person"

  def spec                                                                                                         =
    suite("BlocksApiSpec")(
      test("API examples") {

        for {
          _      <- BlocksApi.put(personTable, Person(id = "id", age = 42)).execute
          person <- BlocksApi.get(personTable)(Person.id === "id").execute
          _      <- BlocksApi.update(personTable)(Person.id === "id")(Person.age.set(42)).execute
          _      <- BlocksApi.deleteFrom(personTable)(Person.id === "id").execute
          _      <- BlocksApi.queryAll[Person](personTable).whereKey(Person.id === "id" && Person.age > 18).execute
        } yield assertTrue(person.isRight)
      },
      suite("Invalid primary key expression throws an exception")(
        test("""for Person.id === "id" && Person.age > 18""") {
          assertIllegalArgument(BlocksApi.get(personTable)(Person.id === "id" && Person.age > 18))(
            "Failed to convert SchemaExpr to PrimaryKeyExpr"
          )
        }
      )
    ).provide(DynamoDBExecutor.test(personTable -> "id"))

  private def assertIllegalArgument(query: => DynamoDBQuery[_, _])(message: String): ZIO[Any, Nothing, TestResult] =
    for {
      exit <- ZIO.attempt(query).exit
    } yield assert(exit)(fails(isSubtype[IllegalArgumentException](hasMessage(containsString(message)))))
}
