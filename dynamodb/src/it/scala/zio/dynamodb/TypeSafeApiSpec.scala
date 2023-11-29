package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import java.time.Instant
import zio.schema.annotation.simpleEnum
import zio.dynamodb
import zio.schema.annotation.noDiscriminator

// This is a place holder suite for the Type Safe API for now, to be expanded upon in the future
object TypeSafeApiSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema: Schema.CaseClass3[String, String, Option[String], Person] = DeriveSchema.gen[Person]
    val (id, surname, forename)                                                    = ProjectionExpression.accessors[Person]
  }

  //@noDiscriminator
  sealed trait MixedSimpleEnum2
  object MixedSimpleEnum2 {
    @simpleEnum
    case object One                                                                extends MixedSimpleEnum2
    @simpleEnum
    case object Two                                                                extends MixedSimpleEnum2
    final case class Three(@dynamodb.Annotations.simpleEnumField() value: Instant) extends MixedSimpleEnum2
  }

  @noDiscriminator
  sealed trait MixedSimpleEnum
  object MixedSimpleEnum {
    final case class One(i: Int, discriminator: String)    extends MixedSimpleEnum
    final case class Two(s: String) extends MixedSimpleEnum
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
      }
    ) @@ nondeterministic

}
