package zio.dynamodb.examples

import zio.aws.core.config
import zio.aws.{ dynamodb, http4s }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import zio.ZIOAppDefault

object Main extends ZIOAppDefault {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema[Person] = DeriveSchema.gen[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- zio.Console.printLine(s"hello $person")
  } yield ()

  override def run = {

    val dynamoDbLayer =
      http4s.Http4sClient.default >>> config.AwsConfig.default >>> dynamodb.DynamoDb.live // uses real AWS dynamodb
    val executorLayer = dynamoDbLayer >>> DynamoDBExecutor.live

    program.provide(executorLayer)
  }
}
