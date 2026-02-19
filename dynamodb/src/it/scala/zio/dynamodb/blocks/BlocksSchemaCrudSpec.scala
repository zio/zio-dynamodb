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
          implicit val cfg: DynamoDBCodecDeriverConfigure[Person] =
            (d: DynamoDBCodecDeriver) => d.withTransientNone(false)
          implicit val schema: Schema[Person]                     = Schema.derived
          val id: Lens[Person, String]                            = $(_.id)
          val name: Lens[Person, String]                          = $(_.name)
        }

        val person = Person("1", "Jones")
        for {
          _                <- DynamoDBQuery2.put(tableName, person).where(Person.id.notExists).execute
          _                <- DynamoDBQuery2.update(tableName)(Person.id === "1")(Person.name.set("Smith")).execute
          found            <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.absolve
          _                <- DynamoDBQuery2.deleteFrom(tableName)(Person.id === "1").execute
          foundAfterDelete <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.maybeFound
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
          _     <- DynamoDBQuery2.put(tableName, person).where(Person.id.notExists).execute
          _     <- DynamoDBQuery2
                     .update(tableName)(Person.id === "1" && Person.year === "2025")(Person.name.set("Smith"))
                     .execute
          found <- DynamoDBQuery2.get(tableName)(Person.id === "1" && Person.year === "2025").execute.absolve
        } yield assertTrue(found == person.copy(name = "Smith"))
      }
    },
    suite("native Map")(
      test("native Map of primitive field update") {
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
            _      <- DynamoDBQuery2.put(tableName, person).execute
            _      <- DynamoDBQuery2.update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(42)).execute
            item   <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
            found  <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.absolve
            _      <- DynamoDBQuery2.update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(21)).execute
            found2 <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.absolve
          } yield assertTrue(
            item == Some(Item("id" -> "1", "map" -> Map("key1" -> 42))),
            found == person.copy(map = Map("key1" -> 42)),
            found2 == person.copy(map = Map("key1" -> 21))
          )
        }
      },
      test("native Map of record") {
        withSingleIdKeyTable { tableName =>
          import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope
//          import zio.dynamodb.AttributeValue._ // bring implicit conversions into scope
          final case class Address(postcode: String, number: Int)
          object Address extends CompanionOptics[Address] {
            implicit val schema: Schema[Address] = Schema.derived
            val postcode: Lens[Address, String]  = $(_.postcode)
            val number: Lens[Address, Int]       = $(_.number)
          }
          final case class Person(id: String, map: Map[String, Address])
          object Person  extends CompanionOptics[Person]  {
            implicit val schema: Schema[Person]                  = Schema.derived
            val id: Lens[Person, String]                         = $(_.id)
            def mapAtKey(key: String): Optional[Person, Address] = $(_.map.atKey(key))
          }

          val person = Person("1", Map.empty)
          for {
            _      <- DynamoDBQuery2.put(tableName, person).execute
            _      <- DynamoDBQuery2
                        .update(tableName)(Person.id === "1")(Person.mapAtKey("postcode1").set(Address("postcode1", 1)))
                        .execute
            item   <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
            found  <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.absolve
            _      <- DynamoDBQuery2
                        .update(tableName)(Person.id === "1")(Person.mapAtKey("postcode1").set(Address("postcode1", 2)))
                        .execute
            found2 <- DynamoDBQuery2.get(tableName)(Person.id === "1").execute.absolve
          } yield assertTrue(
            item == Some(
              Item("id" -> "1", "map" -> Map("postcode1" -> Item("postcode" -> "postcode1", "number" -> 1)))
            ),
            found == person.copy(map = Map("postcode1" -> Address("postcode1", 1))),
            found2 == person.copy(map = Map("postcode1" -> Address("postcode1", 2)))
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
            _    <- DynamoDBQuery2.put(tableName, person).execute
            _    <- DynamoDBQuery2.update(tableName)(Person.id === "1")(Person.maybeMapAtKey("key1").set(42)).execute
            item <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
          } yield assertTrue(item == Some(Item("id" -> "1", "maybeMap" -> Map("key1" -> 42))))
        }
      }
    )
  ) @@ TestAspect.nondeterministic
}
