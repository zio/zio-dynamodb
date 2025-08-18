package zio.dynamodb.blocks

import zio.test._
import zio.dynamodb.DynamoDBLocalSpec
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.PrimaryKey
import zio.dynamodb.syntax._

import zio.blocks.schema._
import zio.dynamodb.Item

object BlocksSchemaCrudSpec extends DynamoDBLocalSpec {
  val spec = suite("Blocks Schema Crud Spec")( // running against DynamoDB in LocalStack
    test("prelude new types") {
      withSingleIdKeyTable { tableName =>
        import zio.prelude._

        type Name = Name.Type

        object Name extends Subtype[String] {
          implicit val schema: Schema[Name] = derive(Schema[String])
        }

        import zio.dynamodb.blocks.BlocksApi._

        final case class Person(id: Name, name: Name)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, Name]          = optic(_.id)
          val name: Lens[Person, Name]        = optic(_.name)
        }

        val person = Person(Name("1"), Name("Jones"))
        for {
          _     <- DynamoDBQuery.put(tableName, person).where(Person.id.notExists).execute
          found <- DynamoDBQuery.get(tableName)(Person.id === Name("1")).execute.absolve
        } yield assertTrue(found == person)
      }
    },
    test("put and get") {
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
    test("mandatory Map field update") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope

        final case class Person(id: String, map: Map[String, Int] = Map.empty)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person]              = Schema.derived
          val id: Lens[Person, String]                     = optic(_.id)
          val map: Lens[Person, Map[String, Int]]          = optic(_.map)
          def mapAtKey(key: String): Optional[Person, Int] = optic(_.map.atKey(key))
        }

        val person = Person("1", Map("key1" -> 1))
        for {
          _    <- DynamoDBQuery.put(tableName, person).execute
          _    <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.mapAtKey("key1").set(42)).execute
          item <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(item == Some(Item("id" -> "1", "map" -> Map("key1" -> 42))))
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
    },
    test("optional Int field update") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope

        final case class Person(id: String, maybeInt: Option[Int] = None)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val maybeInt: Optional[Person, Int] =
            optic(_.maybeInt.when[Some[Int]].value)
        }

        val person = Person("1")
        for {
          _          <- DynamoDBQuery.put(tableName, person).execute
          preUpdate  <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
          _          <- DynamoDBQuery.update(tableName)(Person.id === "1")(Person.maybeInt.set(42)).execute
          postUpdate <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          preUpdate == Some(Item("id" -> "1")),
          postUpdate == Some(Item("id" -> "1", "maybeInt" -> 42))
        )
      }
    },
    test("optional Int field put of Some") {
      withSingleIdKeyTable { tableName =>
        final case class Person(id: String, maybeInt: Option[Int] = None)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val maybeInt: Optional[Person, Int] =
            optic(_.maybeInt.when[Some[Int]].value)
        }

        val person = Person("1", Some(42))
        for {
          _        <- DynamoDBQuery.put(tableName, person).execute
          afterPut <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          afterPut == Some(Item("id" -> "1", "maybeInt" -> 42))
        )
      }
    },
    test("optional Some of Int field get") {
      withSingleIdKeyTable { tableName =>
        import zio.dynamodb.blocks.BlocksApi._ // bring implicit conversions into scope
        final case class Person(id: String, maybeInt: Option[Int] = None)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val maybeInt: Optional[Person, Int] =
            optic(_.maybeInt.when[Some[Int]].value)
        }

        val person = Person("1", Some(42))
        for {
          _        <- DynamoDBQuery.put(tableName, person).execute
          afterPut <- DynamoDBQuery.get(tableName)(Person.id === "1").execute.absolve
        } yield assertTrue(afterPut == person)
      }
    },
    test("optional Int field put of None") {
      withSingleIdKeyTable { tableName =>
        final case class Person(id: String, maybeInt: Option[Int] = None)
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person] = Schema.derived
          val id: Lens[Person, String]        = optic(_.id)
          val maybeInt: Optional[Person, Int] =
            optic(_.maybeInt.when[Some[Int]].value)
        }

        val person = Person("1", None)
        for {
          _        <- DynamoDBQuery.put(tableName, person).execute
          afterPut <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          afterPut == Some(Item("id" -> "1"))
        )
      }
    },
    test("Either put of Right") {
      withSingleIdKeyTable { tableName =>
        final case class Person(id: String, either: Either[String, Int])
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person]           = Schema.derived
          val id: Lens[Person, String]                  = optic(_.id)
          val either: Lens[Person, Either[String, Int]] = optic(_.either)
        }

        val person = Person("1", Right(42))
        for {
          _        <- DynamoDBQuery.put(tableName, person).execute
          afterPut <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          afterPut == Some(Item("id" -> "1", "either" -> Item("Right" -> 42)))
        )
      }
    },
    test("Either put of Left") {
      withSingleIdKeyTable { tableName =>
        final case class Person(id: String, either: Either[String, Int])
        object Person extends CompanionOptics[Person] {
          implicit val schema: Schema[Person]           = Schema.derived
          val id: Lens[Person, String]                  = optic(_.id)
          val either: Lens[Person, Either[String, Int]] = optic(_.either)
        }

        val person = Person("1", Left("Meh"))
        for {
          _        <- DynamoDBQuery.put(tableName, person).execute
          afterPut <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "1")).execute
        } yield assertTrue(
          afterPut == Some(Item("id" -> "1", "either" -> Item("Left" -> "Meh")))
        )
      }
    }
  ) @@ TestAspect.nondeterministic
}
