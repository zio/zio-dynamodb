package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ put, scanAll, scanSome }
import zio.Scope
import zio.test.Spec
import zio.test.assertTrue
import zio.test.TestEnvironment
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.Chunk
import zio.test.TestAspect

object TypeSafeScanAndQuerySpec extends DynamoDBLocalSpec {

  final case class Person(id: String, surname: String, forename: Option[String], age: Int)
  object Person {
    implicit val schema: Schema.CaseClass4[String, String, Option[String], Int, Person] = DeriveSchema.gen[Person]
    val (id, surname, forename, age)                                                    = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("all")(scanAllSpec, scanSomeSpec) @@ TestAspect.nondeterministic

  private val scanAllSpec = suite("scanAll")(
    test("without filter") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("1", "Smith", Some("John"), 21), Person("2", "Brown", None, 42)))
      }
    },
    test("with filter on forename exists") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).filter(Person.forename.exists).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("1", "Smith", Some("John"), 21)))
      }
    },
    test("with filter id in ('1', '2') and age >= 21") {
      withSingleIdKeyTable { tableName =>
        for {
          _      <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _      <- put(tableName, Person("2", "Brown", None, 42)).execute
          stream <- scanAll[Person](tableName).filter(Person.id.in("1", "2") && Person.age > 21).execute
          people <- stream.runCollect
        } yield assertTrue(people == Chunk(Person("2", "Brown", None, 42)))
      }
    }
  )

  private val scanSomeSpec = suite("scanSome")(
    test("without filter pages until lastEvaluatedKey is empty") {
      withSingleIdKeyTable { tableName =>
        for {
          _                               <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _                               <- put(tableName, Person("2", "Brown", None, 42)).execute
          t                               <- scanSome[Person](tableName, 1).execute
          (peopleScan1, lastEvaluatedKey1) = t
          t2                              <- scanSome[Person](tableName, 1).startKey(lastEvaluatedKey1).execute
          (peopleScan2, lastEvaluatedKey2) = t2
          t3                              <- scanSome[Person](tableName, 1).startKey(lastEvaluatedKey2).execute
          (peopleScan3, lastEvaluatedKey3) = t3
        } yield assertTrue(peopleScan1 == Chunk(Person("1", "Smith", Some("John"), 21))) &&
          assertTrue(peopleScan2 == Chunk(Person("2", "Brown", None, 42))) &&
          assertTrue(peopleScan3.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(lastEvaluatedKey2.isDefined) &&
          assertTrue(lastEvaluatedKey3.isEmpty)
      }
    },
    test("with filter pages until items Chunk is empty") {
      withSingleIdKeyTable { tableName =>
        val scanSomeWithFilter = scanSome[Person](tableName, 1).filter(Person.forename.exists)
        for {
          _                               <- put(tableName, Person("1", "Smith", Some("John"), 21)).execute
          _                               <- put(tableName, Person("2", "Brown", None, 42)).execute
          t                               <- scanSomeWithFilter.execute
          (peopleScan1, lastEvaluatedKey1) = t
          t2                              <- scanSomeWithFilter.startKey(lastEvaluatedKey1).execute
          (peopleScan2, lastEvaluatedKey2) = t2
        } yield assertTrue(peopleScan1 == Chunk(Person("1", "Smith", Some("John"), 21))) &&
          assertTrue(peopleScan2.isEmpty) &&
          assertTrue(lastEvaluatedKey1.isDefined) &&
          assertTrue(peopleScan2.isEmpty) // note lastEvaluatedKey2 is still present as item is still read by DynamoDB
      }
    }
  )

}
