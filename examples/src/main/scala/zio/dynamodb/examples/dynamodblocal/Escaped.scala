package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.schema.annotation.fieldName
import zio.{ Console, ZIOAppDefault }

import zio.schema.DeriveSchema

/**
 * Type safe API example
 */
object Escaped extends ZIOAppDefault {

  val x = "foo.`bar-bar`[1].baz"

  val y = $("foo.bar-bar[1].baz")
  println(s"XXXXXXXX y=$y")

  // we use hi level API plus an annotation to re-map field name before storing to DDB
  final case class Escaped(email: String, @fieldName("field-with-dash") fieldWithDash: String)
  object Escaped {
    implicit val schema        = DeriveSchema.gen[Escaped]
    val (email, fieldWithDash) = ProjectionExpression.accessors[Escaped]
  }

  val program = for {
    _ <- createTable("escaped", KeySchema("email"), BillingMode.PayPerRequest)(
           AttributeDefinition.attrDefnString("email")
         ).execute
    _ <- put("escaped", Escaped("email1", "fieldWithDashValue1")).execute
    // I deliberately use low level API to show data at the attribute value level
    // to show field name stored in DDB
    _ <- scanAllItem("escaped").execute
           .tap(_.tap(item => Console.printLine(s"scanAll - escaped=$item")).runDrain)
    _ <- deleteTable("escaped").execute
  } yield ()

  override def run = program.provide(layer)
}
// OUTPUT: scanAll - escaped=AttrMap(Map(field-with-dash -> String(fieldWithDash), subject -> String(subject1), email -> String(email1)))
