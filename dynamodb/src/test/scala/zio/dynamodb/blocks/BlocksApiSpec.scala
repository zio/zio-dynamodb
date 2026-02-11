package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.ConditionExpression.Operand
import zio.dynamodb.{ AttributeValue, ConditionExpression, ProjectionExpression }
import zio.test.{ assertTrue, ZIOSpecDefault }

object BlocksApiSpec extends ZIOSpecDefault {
  import BlocksApi._

  final case class Person(id: String, age: Int)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String] = $(_.id)
    val age: Lens[Person, Int]   = $(_.age)
  }
  val spec = suite("BlocksApiSpec")(
    suite("ConditionExpression")(
      suite("ProjectionExpressionOperand with ValueOperand")(
        test("Person.age > 18") {
          val ce: ConditionExpression[Person] = Person.age > 18

          assertTrue(
            ce ==
              ConditionExpression
                .GreaterThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        },
        test("Person.age >= 18") {
          val ce: ConditionExpression[Person] = Person.age >= 18

          assertTrue(
            ce ==
              ConditionExpression
                .GreaterThanOrEqual(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        },
        test("Person.age === 18") {
          val ce: ConditionExpression[Person] = Person.age === 18

          assertTrue(
            ce ==
              ConditionExpression
                .Equals(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        },
        test("Person.age != 18") {
          val ce: ConditionExpression[Person] = Person.age != 18

          assertTrue(
            ce ==
              ConditionExpression
                .NotEqual(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        },
        test("Person.age < 18") {
          val ce: ConditionExpression[Person] = Person.age < 18

          assertTrue(
            ce ==
              ConditionExpression
                .LessThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        },
        test("Person.age <= 18") {
          val ce: ConditionExpression[Person] = Person.age <= 18

          assertTrue(
            ce ==
              ConditionExpression
                .LessThanOrEqual(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        }
      )
    )
  )

  private def rootMapElement[A](key: String): ProjectionExpression.MapElement[A, ProjectionExpression.Unknown] =
    ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key)

  private def numberValueOperand[A](n: BigDecimal): ConditionExpression.Operand[A, _] =
    Operand.ValueOperand(AttributeValue.Number(n))
}
