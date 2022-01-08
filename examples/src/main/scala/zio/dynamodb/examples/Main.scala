package zio.dynamodb.examples

import io.github.vigoo.zioaws.http4s
import zio.{ App, ExitCode, Has, URIO, ZLayer }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb
import zio.clock.Clock

object Main extends App {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema[Person] = DeriveSchema.gen[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- zio.console.putStrLn(s"hello $person")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val dynamoDbLayer = http4s.default >>> config.default >>> dynamodb.live // uses real AWS dynamodb
    val executorLayer = (dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live

    program.provideCustomLayer(executorLayer).exitCode
  }
}
