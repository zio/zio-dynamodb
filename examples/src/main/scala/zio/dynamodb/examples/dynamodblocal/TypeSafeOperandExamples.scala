package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.ProjectionExpression
import zio.dynamodb.ProjectionExpression.$
import zio.schema.DeriveSchema

import java.time.Instant
import zio.schema.DefaultJavaTimeSchemas

object TypeSafeOperandExamples extends App {

  sealed trait Payment

  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    implicit val schema = DeriveSchema.gen[Payment]
  }
  final case class Person(
    id: String,
    name: String,
    age: Int,
    updated: Instant,
    maybeCount: Option[Int],
    payment: Payment
  )
  object Person extends DefaultJavaTimeSchemas {
    implicit val schema = DeriveSchema.gen[Person]
  }

  val (id, name, age, updated, maybeCount, payment) = ProjectionExpression.accessors[Person]

  val x1 = id === "person_id1"
  val x2 = age ># 10

  val x3 = maybeCount === Some(3)
  val x4 = id === $("a.b")
  val x5 = id ># "1" && payment === Payment.DebitCard
  val x6 = age ># 1
}
