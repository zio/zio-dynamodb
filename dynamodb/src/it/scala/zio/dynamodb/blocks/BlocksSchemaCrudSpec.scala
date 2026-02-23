package zio.dynamodb.blocks

import zio.blocks.schema._
import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope
// no direct imports for DynamoDBQuery
import zio.dynamodb.syntax._
import zio.dynamodb.{ DynamoDBLocalSpec, Item, PrimaryKey }
import zio.test._

object BlocksSchemaCrudSpec extends DynamoDBLocalSpec {
  val spec = suite("Blocks Schema Crud Spec")(
    test("put and get using Blocks composite primary key expression in query API") {
      withIdAndYearKeyTable { tableName =>
        final case class Person(id: String, year: String, name: String)
        object Person extends CompanionOptics[Person] {
          implicit val cfg: DynamoDBCodecDeriverConfigure[Person] =
            (d: DynamoDBCodecDeriver) => d.withTransientNone(false)
          implicit val schema: Schema[Person]                     = Schema.derived
          val id: Lens[Person, String]                            = $(_.id)
          val name: Lens[Person, String]                          = $(_.name)
          val year: Lens[Person, String]                          = $(_.year)
        }

        val person = Person("1", "2026", "Jones")
        for {
          _                <- put(tableName, person).where(Person.id.notExists).execute
          _                <- update(tableName)(Person.id === "1" && Person.year === "2026")(Person.name.set(Person.id))
                                .where(Person.year.between("2025", "2027"))
                                .execute
          found            <- get(tableName)(Person.id === "1" && Person.year === "2026").execute.absolve
          stream1          <- queryAll[Person](tableName)
                                .whereKey(Person.id === "1")
                                .execute
          people1          <- stream1.runCollect
          stream2          <- queryAll[Person](tableName)
                                .whereKey(Person.id === "1" && Person.year > "2025")
                                .execute
          people2          <- stream2.runCollect
          _                <- deleteFrom(tableName)(Person.id === "1" && Person.year === "2026").execute
          foundAfterDelete <- get(tableName)(Person.id === "1" && Person.year === "2026").execute.maybeFound
        } yield assertTrue(
          found == person.copy(name = "1") && people1.size == 1 && people2.size == 1 && foundAfterDelete.isEmpty
        )
      }
    },
    suite("native Map")(
      test("native Map of primitive field update") {
        withSingleIdKeyTable { tableName =>
          final case class Person(id: String, map: Map[String, Int] = Map.empty)
          object Person extends CompanionOptics[Person] {
            implicit val schema: Schema[Person]              = Schema.derived
            val id: Lens[Person, String]                     = $(_.id)
            val map: Lens[Person, Map[String, Int]]          = $(_.map)
            def mapAtKey(key: String): Optional[Person, Int] = $(_.map.atKey(key))
          }

          val person = Person("1", Map.empty)
          for {
            _      <- put(tableName, person).execute
            _      <- update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(42)).execute
            item   <- getItem(tableName, PrimaryKey("id" -> "1")).execute
            found  <- get(tableName)(Person.id === "1").execute.absolve
            _      <- update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(21)).execute
            found2 <- get(tableName)(Person.id === "1").execute.absolve
          } yield assertTrue(
            item == Some(Item("id" -> "1", "map" -> Map("key1" -> 42))),
            found == person.copy(map = Map("key1" -> 42)),
            found2 == person.copy(map = Map("key1" -> 21))
          )
        }
      },
      test("update operations on a list field") {
        withSingleIdKeyTable { tableName =>
          final case class Person(id: String, list: List[Int] = Nil)
          object Person extends CompanionOptics[Person] {
            implicit val schema: Schema[Person] = Schema.derived
            val id: Lens[Person, String]        = optic(_.id)
            val list: Lens[Person, List[Int]]   = optic(_.list)
          }

          val person = Person("1", List(1, 2))
          for {
            _    <- put(tableName, person).execute
            _    <- update(tableName)(Person.id === "1")(Person.list.append(3)).execute
            _    <- update(tableName)(Person.id === "1")(Person.list.appendList(List(4, 5))).execute
            item <- getItem(tableName, PrimaryKey("id" -> "1")).execute
          } yield assertTrue(item == Some(Item("id" -> "1", "list" -> List(1, 2, 3, 4, 5))))
        }
      },
      test("native Map of record") {
        withSingleIdKeyTable { tableName =>
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
            _      <- put(tableName, person).execute
            _      <- update(tableName)(Person.id === "1")(Person.mapAtKey("postcode1").set(Address("postcode1", 1))).execute
            item   <- getItem(tableName, PrimaryKey("id" -> "1")).execute
            found  <- get(tableName)(Person.id === "1").execute.absolve
            _      <- update(tableName)(Person.id === "1")(Person.mapAtKey("postcode1").set(Address("postcode1", 2))).execute
            found2 <- get(tableName)(Person.id === "1").execute.absolve
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
          final case class Person(id: String, maybeMap: Option[Map[String, Int]] = None)
          object Person extends CompanionOptics[Person] {
            implicit val schema: Schema[Person]                   = Schema.derived
            val id: Lens[Person, String]                          = optic(_.id)
            def maybeMapAtKey(key: String): Optional[Person, Int] =
              optic(_.maybeMap.when[Some[Map[String, Int]]].value.atKey(key))
          }

          val person = Person("1", Some(Map()))
          for {
            _    <- put(tableName, person).execute
            _    <- update(tableName)(Person.id === "1")(Person.maybeMapAtKey("key1").set(42)).execute
            item <- getItem(tableName, PrimaryKey("id" -> "1")).execute
          } yield assertTrue(item == Some(Item("id" -> "1", "maybeMap" -> Map("key1" -> 42))))
        }
      }
    )
  ) @@ TestAspect.nondeterministic
}
