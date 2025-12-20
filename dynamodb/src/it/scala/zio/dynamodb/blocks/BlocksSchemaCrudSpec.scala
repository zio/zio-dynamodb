package zio.dynamodb.blocks

import zio.blocks.schema._
import zio.dynamodb.blocks.BlocksApi._
import zio.dynamodb.syntax._
import zio.dynamodb.{ DynamoDBLocalSpec, DynamoDBQuery, Item, PrimaryKey }
import zio.test._

object BlocksSchemaCrudSpec extends DynamoDBLocalSpec {
  val spec = suite("Blocks Schema Crud Spec")(
    test("put and get using Blocks partition key expressions in query API") {
      withSingleIdKeyTable { tableName =>
        final case class Person(id: String, name: String)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = $(_.id)
          val name: Lens[Person, String]      = $(_.name)
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
        final case class Person(id: String, year: String, name: String)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = $(_.id)
          val year: Lens[Person, String]      = $(_.year)
          val name: Lens[Person, String]      = $(_.name)
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
    },
    test("Map field update") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope

        final case class Person(id: String, map: Map[String, Int] = Map.empty)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person]              = Schema.derived
          val id: Lens[Person, String]                     = $(_.id)
          val map: Lens[Person, Map[String, Int]]          = $(_.map)
          def mapAtKey(key: String): Optional[Person, Int] = $(_.map.atKey(key))
        }

        val person = Person("1", Map.empty)
        for {
          _      <- DynamoDBQuery.put(tableName, person).execute
          _      <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(42)).execute
          item   <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
          found  <- DynamoDBQuery.get(tableName)(Person.id === "1").execute.absolve
          _      <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(21)).execute
          found2 <- DynamoDBQuery.get(tableName)(Person.id === "1").execute.absolve
        } yield assertTrue(
          item == Some(Item("id" -> "1", "map" -> Map("key1" -> 42))),
          found == person.copy(map = Map("key1" -> 42)),
          found2 == person.copy(map = Map("key1" -> 21))
        )
      }
    },
    test("optional Map field update") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope

        final case class Person(id: String, maybeMap: Option[Map[String, Int]] = None)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person]                   = Schema.derived
          val id: Lens[Person, String]                          = optic(_.id)
          def maybeMapAtKey(key: String): Optional[Person, Int] =
            optic(_.maybeMap.when[Some[Map[String, Int]]].value.atKey(key))
        }

        val person = Person("1", Some(Map()))
        for {
          _    <- DynamoDBQuery.put(tableName, person).execute
          _    <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.maybeMapAtKey("key1").set(42)).execute
          item <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(item == Some(Item("id" -> "1", "maybeMap" -> Map("key1" -> 42))))
      }
    }
  ) @@ TestAspect.nondeterministic
}
