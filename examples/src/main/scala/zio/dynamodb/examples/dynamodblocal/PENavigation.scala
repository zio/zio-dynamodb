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

  val a: ProjectionExpression[Student, String]        = Student.primary >>> Address.postcode
  val b: ProjectionExpression[Student, List[Address]] = Student.addresses
  val c: ProjectionExpression[Student, Address]       = Student.addresses.elementAt(10)
  val e: ProjectionExpression[Student, String]        = Student.addresses.elementAt(10) >>> Address.postcode
  val f                                               = Student.addressMap

  val y: ProjectionExpression[Student, Int] = Student.addressMap.valueAt("UK") >>> Address.number

  println(s"a = $a")
  println(s"b = $b")
  println(s"c = $c")
  println(s"indexAt(10) = $e")
  println(s"addressMap = $f")

  private val program = for {
    _ <- createTable("student", KeySchema("email", "subject"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email"),
           AttributeDefinition.attrDefnString("subject")
         ).execute
    _ <-
      batchWriteFromStream(ZStream(avi.copy(addressMap = ScalaMap("UK" -> Address("addr1", "postcode1", 1))), adam)) {
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
    _ <- deleteTable("student").execute
  } yield ()

  override def run = program.provide(layer)
}
