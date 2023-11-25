package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._

object TypeSafeApiSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema: Schema.CaseClass3[String, String, Option[String], Person] = DeriveSchema.gen[Person]
    val (id, surname, forename)                                                    = ProjectionExpression.accessors[Person]
  }

  override def spec =
    suite("TypeSafeApiSpec")(
      test("filter on field equality") {
        withSingleKeyTable { tableName =>
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
        withSingleKeyTable { tableName =>
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
        withSingleKeyTable { tableName =>
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
        withSingleKeyTable { tableName =>
          for {
            _  <- DynamoDBQuery.put(tableName, Person("1", "John", Some("Smith"))).execute
            _  <- DynamoDBQuery.put(tableName, Person("2", "Smith", Some("John"))).execute
            xs <- DynamoDBQuery // high level API
                    .forEach(1 to 3)(i => DynamoDBQuery.get[Person](tableName)(Person.id.partitionKey === i.toString))
                    .execute
          } yield assertTrue(xs.size == 3) &&
            assert(xs(0))(isRight) && assert(xs(1))(isRight) &&
            assert(xs(2))(isLeft(isSubtype[DynamoDBError.ValueNotFound](anything)))
        }
      }
    ) @@ nondeterministic

}
