package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb._
import zio.dynamodb.examples.dynamodblocal.DynamoDB._
import zio.schema.DeriveSchema
import zio.{ Console, ZIOAppDefault }

object PEExperiment extends ZIOAppDefault {

  final case class RecordWithTtl(
    email: String,
    // @fieldName("alt_ttl")
    ttlX: Option[Long]
  )

  object RecordWithTtl {
    implicit val schema = DeriveSchema.gen[RecordWithTtl]
    val (email, myTtl)  = ProjectionExpression.accessors[RecordWithTtl]
  }

  private val program = for {
    _   <- createTable("withttl", KeySchema("email"), BillingMode.PayPerRequest)(
             AttributeDefinition.attrDefnString("email")
           ).execute
    _   <- put("withttl", RecordWithTtl("email1", Some(10L))).execute
    rec <- get[RecordWithTtl]("withttl", PrimaryKey("email" -> "email1")).execute.absolve
    s   <- scanAll[RecordWithTtl]("withttl").filter(RecordWithTtl.myTtl === Some(10L)).execute.flatMap(_.runCollect)
    _   <- Console.printLine(s"XXXXXXXXXXXXX $s")
  } yield rec

  override def run = program.provide(layer)
}
