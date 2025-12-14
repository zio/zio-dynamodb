package zio.dynamodb.blocks

import zio.blocks.schema._
import zio.dynamodb.syntax._
import zio.dynamodb.{ DynamoDBLocalSpec, DynamoDBQuery }
import zio.test._

object BlocksSchemaCrudSpec extends DynamoDBLocalSpec {
  val spec = suite("Blocks Schema Crud Spec")(
    test("put and get using Blocks partition key expressions in query API") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._

        final case class Person(id: String, name: String)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val name: Lens[Person, String]      = optic(_.name)
        }

        val person = Person("1", "Jones")
        for {
          _                <- DynamoDBQuery.put(tableName, person).where(Person.id.notExists).execute
          _                <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.name.set("Smith")).execute
          found            <- DynamoDBQuery.get(tableName)(Person.id === "1").execute.absolve
          _                <- DynamoDBQuery.deleteFrom(tableName)(Person.id === "1").execute
          foundAfterDelete <- DynamoDBQuery.get(tableName)(Person.id === "1").execute.maybeFound
        } yield assertTrue(found == person.copy(name = "Smith") && foundAfterDelete.isEmpty)
      }
    },
    test("put and get using Blocks composite primary key expression in query API") {
      withIdAndYearKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._

        final case class Person(id: String, year: String, name: String)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val year: Lens[Person, String]      = optic(_.year)
          val name: Lens[Person, String]      = optic(_.name)
        }

        val person = Person("1", "2025", "Jones")
        for {
          _     <- DynamoDBQuery.put(tableName, person).where(Person.id.notExists).execute
          _     <- DynamoDBQuery
                     .update(tableName)(Person.id === "1" && Person.year === "2025")(Person.name.set("Smith"))
                     .execute
          found <- DynamoDBQuery.get(tableName)(Person.id === "1" && Person.year === "2025").execute.absolve
        } yield assertTrue(found == person.copy(name = "Smith"))
      }
    }
  ) @@ TestAspect.nondeterministic
}
