package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.model.Student._
import zio.dynamodb.examples.model._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.stream.ZStream
import zio.{ Console, ZIOAppDefault }

import scala.collection.immutable.{ Map => ScalaMap }

import java.time.Instant

/**
 * Type safe API example
 */
object PENavigation extends ZIOAppDefault {

  val enrollmentDateTyped: ProjectionExpression[Student, Option[Instant]] = enrollmentDate

  private val program = for {
    _ <- batchWriteFromStream(ZStream(avi.copy(addressMap = ScalaMap("UK" -> Address("addr1", "postcode1"))), adam)) {
           student =>
             put("student", student)
         }.runDrain
    _ <- scanAll[Student]("student")
           .filter(
             Student.addresses.elementAt(0) >>> Address.postcode === "postcode2" &&
               Student.addressMap.valueAt("UK") >>> Address.postcode === "postcode1"
           )
           .execute
           .tap(_.tap(student => Console.printLine(s"scanAll - student=$student")).runDrain)
  } yield ()

  override def run = program.provide(dynamoDBExecutorLayer, studentTableLayer)
}
