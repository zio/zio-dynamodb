package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.assertTrue
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.Scope

object EitherSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, firstName: String, address: Either[String, List[String]])
  object Person {
    implicit val schema: Schema.CaseClass3[String, String, Either[String, List[String]], Person] =
      DeriveSchema.gen[Person]
    final val (id, firstName, address)                                                           = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[Environment with Scope, Any] =
    suite("EitherSpec")(
      test("Person with Either") {
        withSingleIdKeyTable { tableName =>
          val originalPerson = Person("1", "Smith", Right(List("123 Main St", "456 Elm St")))
          for {
            _       <- put(tableName, originalPerson).execute
            updated <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
          } yield assertTrue(updated == originalPerson)
        }

      }
    ) @@ TestAspect.nondeterministic

}
