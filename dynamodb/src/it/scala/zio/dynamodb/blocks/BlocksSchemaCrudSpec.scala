package zio.dynamodb.blocks

import zio.test._
import zio.dynamodb.DynamoDBLocalSpec
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.syntax._

import zio.blocks.schema._

object BlocksSchemaCrudSpec extends DynamoDBLocalSpec {
  val spec = suite("Blocks Schema Crud Spec")( // running against DynamoDB in LocalStack
    test("put and get") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope

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
    }
  )
}
