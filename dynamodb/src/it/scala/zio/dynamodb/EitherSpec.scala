package zio.dynamodb

import zio.schema.{ DeriveSchema, Fallback, Schema }
import zio.test._
import zio.test.assertTrue
import zio.dynamodb.DynamoDBQuery.{ get, getItem, put }
import zio.Scope

object EitherSpec extends DynamoDBLocalSpec {

  final case class Person(id: String, firstName: String, address: Either[String, List[String]])
  object Person {
    implicit val schema: Schema.CaseClass3[String, String, Either[String, List[String]], Person]             =
      DeriveSchema.gen[Person]
    implicit def fallbackEither[A, B](implicit schemaA: Schema[A], schemaB: Schema[B]): Schema[Either[A, B]] =
      Schema.Fallback(schemaA, schemaB).transform(_.toEither, Fallback.fromEither)
    final val (id, firstName, address)                                                                       = ProjectionExpression.accessors[Person]
  }

  final case class Person2(id: String, firstName: String, address: Either[String, Either[Int, List[String]]])
  object Person2 {
    implicit val schema: Schema.CaseClass3[String, String, Either[String, Either[Int, List[String]]], Person2] =
      DeriveSchema.gen[Person2]
    implicit def fallbackEither[A, B](implicit schemaA: Schema[A], schemaB: Schema[B]): Schema[Either[A, B]]   =
      Schema.Fallback(schemaA, schemaB).transform(_.toEither, Fallback.fromEither)
    final val (id, firstName, address)                                                                         = ProjectionExpression.accessors[Person2]
  }

  override def spec: Spec[Environment with Scope, Any] =
    suite("EitherSpec")(
      test("Person with Either") {
        withSingleIdKeyTable { tableName =>
          val originalPerson1 = Person("1", "Smith", Right(List("123 Main St", "456 Elm St")))
          val originalPerson2 = Person("2", "Smith", Left("123 Main St"))
          for {
            _        <- put(tableName, originalPerson1).execute
            x1       <- getItem(tableName, PrimaryKey("id" -> "1")).execute
            _         = println(s"XXXXXXXXX x1: $x1")
// XXXXXXXXX x1: Some(AttrMap(Map(firstName -> String(Smith), address -> List(Chunk(String(123 Main St),String(456 Elm St))), id -> String(1))))
            updated1 <- get(tableName)(Person.id.partitionKey === "1").execute.absolve
            _        <- put(tableName, originalPerson2).execute
            x2       <- getItem(tableName, PrimaryKey("id" -> "2")).execute
// XXXXXXXXX x2: Some(AttrMap(Map(firstName -> String(Smith), address -> String(123 Main St), id -> String(2))))
            _         = println(s"XXXXXXXXX x2: $x2")
            updated2 <- get(tableName)(Person.id.partitionKey === "2").execute.absolve
          } yield assertTrue(updated1 == originalPerson1, updated2 == originalPerson2)
        }
      },
      test("Person with nested Either") {
        withSingleIdKeyTable { tableName =>
          val originalPerson1 = Person2("1", "Smith", Right(Right(List("123 Main St", "456 Elm St"))))
          val originalPerson2 = Person2("2", "Smith", Right(Left(1)))
          val originalPerson3 = Person2("3", "Smith", Left("123 Main St"))
          for {
            _        <- put(tableName, originalPerson1).execute
            x1       <- getItem(tableName, PrimaryKey("id" -> "1")).execute
            _         = println(s"XXXXXXXXX x1: $x1")
            updated1 <- get(tableName)(Person2.id.partitionKey === "1").execute.absolve
            _        <- put(tableName, originalPerson2).execute
            x2       <- getItem(tableName, PrimaryKey("id" -> "2")).execute
            _         = println(s"XXXXXXXXX x2: $x2")
            updated2 <- get(tableName)(Person2.id.partitionKey === "2").execute.absolve
            _        <- put(tableName, originalPerson3).execute
            x3       <- getItem(tableName, PrimaryKey("id" -> "3")).execute
            _         = println(s"XXXXXXXXX x3: $x3")
            updated3 <- get(tableName)(Person2.id.partitionKey === "3").execute.absolve
          } yield assertTrue(updated1 == originalPerson1, updated2 == originalPerson2, updated3 == originalPerson3)
        }

      }
    ) @@ TestAspect.nondeterministic

}
