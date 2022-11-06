package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.stream.ZStream
import zio.{ console, App, ExitCode, URIO }

object TypeSafeOptimisticLockingExample extends App {

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
    before <- get[Student]("student", primaryKey("avi@gmail.com", "maths")).execute.right

    // STEP 2 - simulate contention
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
           altPayment.set(Payment.CreditCard) + version.add(1)
         }.execute

    // STEP 3 - make updates and check version is still the same
    _ <- update[Student]("student", primaryKey("avi@gmail.com", "maths")) {
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
