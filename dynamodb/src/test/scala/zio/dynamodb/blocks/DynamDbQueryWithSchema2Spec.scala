package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema }
import zio.dynamodb.DynamoDBExecutor
import zio.test.{ assertTrue, ZIOSpecDefault }

object DynamDbQueryWithSchema2Spec extends ZIOSpecDefault {
  final case class Person(id: String, age: Int, list: List[String] = Nil)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String]                 = $(_.id)
    val age: Lens[Person, Int]                   = $(_.age)
    val list: Lens[Person, List[String]]         = $(_.list)
    def atList(i: Int): Optional[Person, String] = $(_.list.at(i))
  }
  val personTable = "person"

  import BlocksApi._

  def spec =
    suite("DynamDbQueryWithSchema2Spec")(
      test("get using Schema2") {

        for {
          _      <- BlocksApi.put(personTable, Person(id = "id", age = 42)).execute
          person <- BlocksApi.get(personTable)(Person.id === "id").execute
          _      <- BlocksApi.update(personTable)(Person.id === "id")(Person.age.set(42)).execute
          _      <- BlocksApi.deleteFrom(personTable)(Person.id === "id").execute
          _      <- BlocksApi.queryAll[Person](personTable).whereKey(Person.id === "id" && Person.age > 18).execute
        } yield assertTrue(person.isRight)
      }
    ).provide(DynamoDBExecutor.test(personTable -> "id"))
}
