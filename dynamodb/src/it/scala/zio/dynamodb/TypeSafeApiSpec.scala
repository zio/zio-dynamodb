package zio.dynamodb

import zio.Scope
import zio.ZIO
import zio.dynamodb.DynamoDB.{ dynamoDBExecutorLayer }
import zio.dynamodb.DynamoDBQuery
import zio.schema.DeriveSchema
import zio.test.ZIOSpecDefault
import zio.test._
import zio.test.TestAspect._

object TypeSafeApiSpec extends ZIOSpecDefault {

  def withIdKeyOnly(tableName: String) =
    DynamoDBQuery.createTable(tableName, KeySchema("id"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("id")
    )

  def withPartitionAndSortKey(tableName: String) =
    DynamoDBQuery.createTable(tableName, KeySchema("email", "subject"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("email"),
      AttributeDefinition.attrDefnString("subject")
    )

  def managedTable(tableDefinition: String => DynamoDBQuery.CreateTable) =
    ZIO
      .acquireRelease(
        for {
          tableName <- zio.Random.nextUUID.map(_.toString)
          _         <- tableDefinition(tableName).execute
        } yield tableName
      )(tableName => DynamoDBQuery.deleteTable(tableName).execute.orDie)

  def withSingleKeyOnlyTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ) =
    ZIO.scoped {
      managedTable(withIdKeyOnly).flatMap(f)
    }

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema         = DeriveSchema.gen[Person]
    val (id, surname, forename) = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("TypeSafeApiSpec")(
      test("filter on partition key equality") {
        withSingleKeyOnlyTable { tableName =>
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
        withSingleKeyOnlyTable { tableName =>
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
        withSingleKeyOnlyTable { tableName =>
          for {
            _      <- DynamoDBQuery.put(tableName, Person("1", "Smith", Some("John"))).execute
            stream <- DynamoDBQuery
                        .scanAll[Person](tableName)
                        .filter(Person.forename.notExists)
                        .execute
            people <- stream.runCollect
          } yield assertTrue(people.size == 0)
        }
      }
    ).provide(dynamoDBExecutorLayer) @@ nondeterministic

}
