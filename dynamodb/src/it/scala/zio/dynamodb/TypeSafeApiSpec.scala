package zio.dynamodb

import zio.dynamodb.DynamoDB.{ dynamoDBExecutorLayer, personTableLayer }
import zio.test.ZIOSpecDefault
import zio.Scope
import zio.test._
import zio.schema.DeriveSchema

object TypeSafeApiSpec extends ZIOSpecDefault {

  final case class Person(id: String, surname: String, forename: Option[String])
  object Person {
    implicit val schema         = DeriveSchema.gen[Person]
    val (id, surname, forename) = ProjectionExpression.accessors[Person]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("TypeSafeApiSpec")(
      test("filter on partition equality") {
        for {
          _      <- DynamoDBQuery.put("person", Person("1", "Smith", Some("John"))).execute
          stream <- DynamoDBQuery
                      .scanAll[Person]("person")
                      .filter(Person.surname === "Smith")
                      .execute
          people <- stream.runCollect
        } yield assertTrue(people.size == 1)
      }.provide(dynamoDBExecutorLayer, personTableLayer) @@ TestAspect.ignore,
      test("filter on optional field exists") {
        for {
          _      <- DynamoDBQuery.put("person", Person("1", "Smith", Some("John"))).execute
          stream <- DynamoDBQuery
                      .scanAll[Person]("person")
                      .filter(Person.forename.exists)
                      .execute
          people <- stream.runCollect
        } yield assertTrue(people.size == 1)
      }.provide(dynamoDBExecutorLayer, personTableLayer) @@ TestAspect.ignore,
      test("filter on optional field exists") {
        for {
          _      <- DynamoDBQuery.put("person", Person("1", "Smith", Some("John"))).execute
          stream <- DynamoDBQuery
                      .scanAll[Person]("person")
                      .filter(Person.forename.notExists)
                      .execute
          people <- stream.runCollect
        } yield assertTrue(people.size == 0)
      }.provide(dynamoDBExecutorLayer, personTableLayer)
    )

}
