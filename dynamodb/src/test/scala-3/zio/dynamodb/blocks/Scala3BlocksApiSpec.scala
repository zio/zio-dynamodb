package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Schema }
import zio.dynamodb.proofs.Addable
import zio.dynamodb.{ ProjectionExpression, ProjectionExpressionOps, ToAttributeValue, UpdateExpression }
import zio.test.{ assertTrue, ZIOSpecDefault }

object Scala3BlocksApiSpec extends ZIOSpecDefault {

  case class Person(
    name: String,
    age: Int,
    set: Set[Int] = Set.empty,
    map: Map[String, Int] = Map.empty,
    list: List[Int] = Nil
  )
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]      = Schema.derived
    val name: Optic[Person, String]          = $(_.name)
    val age: Optic[Person, Int]              = $(_.age)
    val set: Optic[Person, Set[Int]]         = $(_.set)
    val map: Optic[Person, Map[String, Int]] = $(_.map)
    val list: Optic[Person, List[Int]]       = $(_.list)
  }

  override def spec =
    suite("Scala 3 allows syntax")(
      test("using Scala 3 extension methods syntax") {
        import BlocksApi.*
        Person.age.add(1)
        Person.set.addSet(Set(1))
        Person.set.deleteFromSet(Set(1))

        assertTrue(1 == 1)
      }
    )
}
