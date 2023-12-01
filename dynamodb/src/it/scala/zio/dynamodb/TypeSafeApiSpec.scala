package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.schema.annotation.noDiscriminator

// This is a place holder suite for the Type Safe API for now, to be expanded upon in the future
object TypeSafeApiSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema: Schema.CaseClass3[String, String, Option[String], Person] = DeriveSchema.gen[Person]
    val (id, surname, forename)                                                    = ProjectionExpression.accessors[Person]
  }

  @noDiscriminator
  sealed trait NoDiscrinatorSumType {
    def id: String
  }
  object NoDiscrinatorSumType       {
    final case class One(id: String, i: Int) extends NoDiscrinatorSumType
    object One {
      implicit val schema = DeriveSchema.gen[One]
      val (id, i)         = ProjectionExpression.accessors[One]
    }
    final case class Two(id: String, j: Int) extends NoDiscrinatorSumType
    object Two {
      implicit val schema = DeriveSchema.gen[Two]
      val (id, j)         = ProjectionExpression.accessors[Two]
    }
  }

  override def spec =
    suite("TypeSafeApiSpec")(
      test("filter on field equality") {
        withSingleIdKeyTable { tableName =>
          for {
            _      <- DynamoDBQuery.put(tableName, Person("1", "Smith", Some("John"))).execute
            stream <- DynamoDBQuery
                        .scanAll[Person](tableName)
                        .filter(Person.surname === "Smith")
                        .execute
            people <- stream.runCollect
          } yield assertTrue(people.size == 1)
        }
      },
      test("filter on optional field exists") {
        withSingleIdKeyTable { tableName =>
          for {
            _      <- DynamoDBQuery.put(tableName, Person("1", "Smith", Some("John"))).execute
            stream <- DynamoDBQuery
                        .scanAll[Person](tableName)
                        .filter(Person.forename.exists)
                        .execute
            people <- stream.runCollect
          } yield assertTrue(people.size == 1)
        }
      },
      test("filter on optional field not exists") {
        withSingleIdKeyTable { tableName =>
          for {
            _      <- DynamoDBQuery.put(tableName, Person("1", "Smith", Some("John"))).execute
            stream <- DynamoDBQuery
                        .scanAll[Person](tableName)
                        .filter(Person.forename.notExists)
                        .execute
            people <- stream.runCollect
          } yield assertTrue(people.size == 0)
        }
      },
      test("forEach returns a left of ValueNotFound when no item is found") {
        withSingleIdKeyTable { tableName =>
          for {
            _  <- DynamoDBQuery.put(tableName, Person("1", "John", Some("Smith"))).execute
            _  <- DynamoDBQuery.put(tableName, Person("2", "Smith", Some("John"))).execute
            xs <- DynamoDBQuery
                    .forEach(1 to 3)(i => DynamoDBQuery.get[Person](tableName)(Person.id.partitionKey === i.toString))
                    .execute
          } yield assertTrue(xs.size == 3) &&
            assert(xs(0))(isRight) && assert(xs(1))(isRight) &&
            assert(xs(2))(isLeft(isSubtype[DynamoDBError.ValueNotFound](anything)))
        }
      },
      test("@noDiscriminator sum type round trip") {
        withSingleIdKeyTable { tableName =>
          for {
            _ <- DynamoDBQuery.put(tableName, NoDiscrinatorSumType.One("id1", 42)).execute
            a <- DynamoDBQuery.get(tableName)(NoDiscrinatorSumType.One.id.partitionKey === "id1").execute.absolve
          } yield assertTrue(a == NoDiscrinatorSumType.One("id1", 42))
        }
      }
    ) @@ nondeterministic

}
