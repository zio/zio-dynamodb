package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.ProjectionExpression.$
import zio.schema.DeriveSchema

import java.time.Instant
import zio.schema.DefaultJavaTimeSchemas

object TypeSafeOperandExamples extends App {

  @enumOfCaseObjects
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

  val id2: ProjectionExpression[String] = id

  val x1 =
    id === "person_id1"

  val x3 = maybeCount === Some(3)
  val x4 = id === $("a.b")

//  val set = payment.set2[Payment](Payment.PayPal)

  val set1 = payment.set[Payment](Payment.PayPal)
  println(set1)

  val set2 = payment.set[Person](Person("id1", "avi", 1, Instant.now, None, Payment.PayPal))
  println(set2)

  // the type String must be a Int in order to use this operator
  // val x = age === id

}
