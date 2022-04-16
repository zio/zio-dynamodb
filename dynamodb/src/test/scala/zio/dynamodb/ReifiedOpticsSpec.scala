package zio.dynamodb

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.schema.DeriveSchema
import zio.test.Assertion.equalTo
import zio.test._

object ReifiedOpticsSpec extends DefaultRunnableSpec {

  @enumOfCaseObjects
  sealed trait Payment
  object Payment {
    final case object DebitCard  extends Payment
    final case object CreditCard extends Payment
    final case object PayPal     extends Payment

    val schema = DeriveSchema.gen[Payment]
  }
  final case class Person(name: String, age: Int, payment: Payment)

  object Person {
    implicit val schema      = DeriveSchema.gen[Person]
    val (name, age, payment) = ProjectionExpression.accessors[Person]
  }

  override def spec: ZSpec[Environment, Failure] =
    suite("reified optics suite")(
      test("name Lens results is a valid projection expression") {
        val pe: ProjectionExpression = Person.name
        assert(pe)(equalTo(ProjectionExpression.MapElement(ProjectionExpression.Root, "name")))
      },
      test("age Lens results is a valid projection expression") {
        val pe: ProjectionExpression = Person.age
        assert(pe)(equalTo(ProjectionExpression.MapElement(ProjectionExpression.Root, "age")))
      },
      test("payment Prism results is a valid projection expression") {
        val pe: ProjectionExpression = Person.payment
        assert(pe)(equalTo(ProjectionExpression.MapElement(ProjectionExpression.Root, "payment")))
      }
    )
}
