package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.schema.annotation.noDiscriminator
import zio.dynamodb.DynamoDBQuery._
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException

// This is a place holder suite for the Type Safe API for now, to be expanded upon in the future
object TypeSafeApiCrudSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String], age: Int)
  object Person {
    implicit val schema: Schema.CaseClass4[String, String, Option[String], Int, Person] = DeriveSchema.gen[Person]
    val (id, surname, forename, age)                                                    = ProjectionExpression.accessors[Person]
  }

  @noDiscriminator
  sealed trait NoDiscrinatorSumType {
    def id: String
  }
  object NoDiscrinatorSumType       {
    final case class One(id: String, i: Int) extends NoDiscrinatorSumType
    object One {
      implicit val schema: Schema.CaseClass2[String, Int, One] = DeriveSchema.gen[One]
      val (id, i)                                              = ProjectionExpression.accessors[One]
    }
    final case class Two(id: String, j: Int) extends NoDiscrinatorSumType
    object Two {
      implicit val schema: Schema.CaseClass2[String, Int, Two] = DeriveSchema.gen[Two]
      val (id, j)                                              = ProjectionExpression.accessors[Two]
    }
  }

  override def spec = suite("all")(putSuite, updateSuite) @@ nondeterministic

  private val putSuite =
    suite("TypeSafeApiSpec")(
      test("simple put and get round trip") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          for {
            _ <- put(tableName, person).execute
            p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == person)
        }
      },
      test("put with condition expression that id exists fails for empty database") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.notExists).execute.exit
          } yield exit
          assertZIO(exit)(fails(isSubtype[ConditionalCheckFailedException](anything)))
        }
      },
      test("put with condition expression that id exists when there is a record succeeds") {
        withSingleIdKeyTable { tableName =>
          val person = Person("1", "Smith", Some("John"), 21)
          val exit   = for {
            _    <- put(tableName, person).execute
            exit <- put(tableName, person).where(Person.id.exists).execute.exit
          } yield exit
          assertZIO(exit)(succeeds(anything))
        }
      },
      test("put with compound condition expression succeeds") {
        withSingleIdKeyTable { tableName =>
          val person        = Person("1", "Smith", None, 21)
          val personUpdated = person.copy(forename = Some("John"))
          for {
            _ <- put(tableName, person).execute
            _ <-
              put(tableName, personUpdated)
                .where(Person.id.exists && Person.surname === "Smith" && Person.forename.notExists && Person.age > 20)
                .execute
            p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(p == personUpdated)
        }
      }
    )

  private val updateSuite = suite("update suite")(
    test("updates a single field with an update expression when record exists") {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John"))).execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    },
    test(
      "updates a single field with an update expression restricted by a compound condition expression when record exists"
    ) {
      withSingleIdKeyTable { tableName =>
        val person   = Person("1", "Smith", None, 21)
        val expected = person.copy(forename = Some("John"))
        for {
          _ <- put(tableName, person).execute
          _ <- update(tableName)(Person.id.partitionKey === "1")(Person.forename.set(Some("John")))
                 .where(Person.surname === "Smith" && Person.forename.notExists)
                 .execute
          p <- get[Person](tableName)(Person.id.partitionKey === "1").execute.absolve
        } yield assertTrue(p == expected)
      }
    }
  )

}
