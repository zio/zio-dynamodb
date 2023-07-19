package zio.dynamodb.examples

import zio.aws.core.config
import zio.aws.{ dynamodb, netty }
import zio.dynamodb.DynamoDBQuery.{ get2, put }
import zio.dynamodb.{ DynamoDBExecutor }
import zio.schema.{ DeriveSchema, Schema }
import zio.ZIOAppDefault
import zio.dynamodb.ProjectionExpression

object Main extends ZIOAppDefault {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]

    val (id, firstName) = ProjectionExpression.accessors[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("personTable", examplePerson).execute
    person <- get2("personTable", Person.id.primaryKey === 1).execute
    _      <- zio.Console.printLine(s"hello $person")
  } yield ()

  override def run =
    program.provide(
      netty.NettyHttpClient.default,
      config.AwsConfig.default, // uses real AWS dynamodb
      dynamodb.DynamoDb.live,
      DynamoDBExecutor.live
    )
}
