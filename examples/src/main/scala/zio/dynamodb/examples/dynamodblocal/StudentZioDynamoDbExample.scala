package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery.{ createTable, put }
import zio.dynamodb._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.stream.ZStream
import zio.{ console, App, ExitCode, URIO }

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 */
object StudentZioDynamoDbExample extends App {

  private val program = for {
    _ <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
    _ <- batchWriteFromStream(ZStream(avi, adam)) { student =>
           put("student", student)
         }.runDrain
    _ <- put("student", avi.copy(payment = Payment.CreditCard)).execute
    _ <- batchReadFromStream("student", ZStream(avi, adam))(s => primaryKey(s.email, s.subject))
           .tap(student => console.putStrLn(s"student=$student"))
           .runDrain
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
