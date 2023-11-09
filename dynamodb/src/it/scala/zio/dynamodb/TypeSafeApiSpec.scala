package zio.dynamodb

import zio.dynamodb.DynamoDB.{ dynamoDBExecutorLayer, personTableLayer }
import zio.dynamodb.DynamoDBQuery
import zio.test.ZIOSpecDefault
import zio.Scope
import zio.test._
import zio.schema.DeriveSchema
import zio.ZIO

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
        } yield TableName(tableName)
      )(tName => DynamoDBQuery.deleteTable(tName.value).execute.orDie)

  def withIdKeyOnlyTable(
    f: String => ZIO[DynamoDBExecutor, Throwable, TestResult]
  ) =
    ZIO.scoped {
      managedTable(withIdKeyOnly).flatMap { table =>
        f(table.value)
      }
    }

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema         = DeriveSchema.gen[Person]
    val (id, surname, forename) = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("TypeSafeApiSpec")(
      test("filter on partition equality") {
        withIdKeyOnlyTable { tableName =>
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
        withIdKeyOnlyTable { tableName =>
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
        withIdKeyOnlyTable { tableName =>
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
    ).provide(dynamoDBExecutorLayer)

}
