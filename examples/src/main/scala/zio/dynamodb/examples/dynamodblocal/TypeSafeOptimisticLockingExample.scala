package zio.dynamodb.examples.dynamodblocal

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
import software.amazon.awssdk.regions.Region
import zio.blocking.Blocking
import zio.clock.Clock
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb._
import zio.dynamodb.examples.LocalDdbServer
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.stream.ZStream
import zio.{ console, App, ExitCode, Has, URIO, ZLayer }

import java.net.URI

/**
 * An equivalent app to [[StudentJavaSdkExample]] but using `zio-dynamodb` - note the reduction in boiler plate code!
 * It also uses the type safe query and update API.
 */
object TypeSafeOptimisticLockingExample extends App {

  private val awsConfig = ZLayer.succeed(
    config.CommonAwsConfig(
      region = None,
      credentialsProvider = SystemPropertyCredentialsProvider.create(),
      endpointOverride = None,
      commonClientConfig = None
    )
  )

  private val dynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    (http4s.default ++ awsConfig) >>> config.configured() >>> dynamodb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    }

  private val layer = ((dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live) ++ (ZLayer
    .identity[Has[Blocking.Service]] >>> LocalDdbServer.inMemoryLayer)

  def optimisticUpdateStudent(primaryKey: PrimaryKey)(actions: Action[Student]) =
    for {
      before <- get[Student]("student", primaryKey).execute.right
      _      <- update[Student]("student", primaryKey) {
                  actions + version.add(1)
                }.where(Student.version === before.version).execute
    } yield ()

  private val program = for {
    _      <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
                AttributeDefinition.attrDefnString("email"),
                AttributeDefinition.attrDefnString("subject")
              ).execute
    _      <- batchWriteFromStream(ZStream(avi, adam)) { student =>
                put("student", student)
              }.runDrain

    // STEP 1 - get version
    before <- get[Student]("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")).execute.right

    // STEP 2 - simulate contention
    _ <- update[Student]("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
           version.add(1)
         }.execute

    // STEP 3 - make updates and check version is still the same
    _ <- update[Student]("student", PrimaryKey("email" -> "avi@gmail.com", "subject" -> "maths")) {
           altPayment.set(Payment.PayPal) + version.add(1)
         }.where(Student.version === before.version)
           .execute
           .either
           .tap(error => console.putStrLn(s"optimistic locking error=$error"))

    _ <- scanAll[Student]("student").execute
           .tap(_.tap(student => console.putStrLn(s"scanAll - student=$student")).runDrain)

  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = program.provideCustomLayer(layer).exitCode
}
