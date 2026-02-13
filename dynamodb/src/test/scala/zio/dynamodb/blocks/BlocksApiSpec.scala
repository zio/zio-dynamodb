package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema, SchemaExpr }
import zio.dynamodb.ConditionExpression.Operand
import zio.dynamodb.KeyConditionExpr.{ CompositePrimaryKeyExpr, PartitionKeyEquals, SortKeyEquals }
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.{
  blocks,
  AttributeValue,
  ConditionExpression,
  KeyConditionExpr,
  PartitionKey,
  ProjectionExpression,
  SortKey,
  UpdateExpression
}
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

object BlocksApiSpec extends ZIOSpecDefault {
  import BlocksApi._

  final case class Person(id: String, age: Int, list: List[String], map: Map[String, Int])
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String]                 = $(_.id)
    val age: Lens[Person, Int]                   = $(_.age)
    val list: Lens[Person, List[String]]         = $(_.list)
    def atList(i: Int): Optional[Person, String] = $(_.list.at(i))
  }

  object PersonId extends Newtype[String] {
    implicit val x: Schema[blocks.BlocksApiSpec.PersonId.Type] =
      Schema[String].transform(s => PersonId(s), (personId: PersonId) => PersonId.unwrap(personId))
  }
  type PersonId = PersonId.Type

  final case class PersonWithPreludeNewtype(personId: PersonId, age: Int)
  object PersonWithPreludeNewtype extends CompanionOptics[PersonWithPreludeNewtype] {
    implicit val schema: Schema[PersonWithPreludeNewtype]  = Schema.derived
    val personId: Lens[PersonWithPreludeNewtype, PersonId] = $(_.personId)
    val age: Lens[PersonWithPreludeNewtype, Int]           = $(_.age)
  }

  val spec = suite("BlocksApiSpec should")(
    suite("automatically convert SchemaExpr to a ConditionExpression")(
      suite("conjunction")(
        test("Person.age > 18 && Person.age < 65") {
          val ce: ConditionExpression[Person] = Person.age > 18 && Person.age < 65

          assertTrue(
            ce ==
              ConditionExpression.And(
                ConditionExpression.GreaterThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                ),
                ConditionExpression.LessThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(65))
                )
              )
          )
        }
      ),
      suite("disjunction")(
        test("Person.age < 18 || Person.age > 65") {
          val ce: ConditionExpression[Person] = Person.age < 18 || Person.age > 65

          assertTrue(
            ce ==
              ConditionExpression.Or(
                ConditionExpression.LessThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                ),
                ConditionExpression.GreaterThan(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(65))
                )
              )
          )
        }
      ),
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
    ), // end ConditionExpression suite
    suite("automatically convert SchemaExpr to a KeyCondition")(
      test("Person.id === 'abc'") {
        val schemaExpr: SchemaExpr[Person, Boolean] = Person.id === "abc"
        val kce: KeyConditionExpr[Person]           = schemaExpr

        assertTrue(
          kce == PartitionKeyEquals(PartitionKey("id"), AttributeValue.String("abc"))
        )
      },
      test("PersonWithPreludeNewType.personId === 'abc'") {

        val schemaExpr: SchemaExpr[PersonWithPreludeNewtype, Boolean] =
          PersonWithPreludeNewtype.personId === PersonId("abc")
        val kce: KeyConditionExpr[PersonWithPreludeNewtype]           = schemaExpr

        assertTrue(
          kce == PartitionKeyEquals(PartitionKey("personId"), AttributeValue.String("abc"))
        )
      },
      test("Person.id === 'abc' && Person.age == 18") {
        val kce: KeyConditionExpr[Person] = (Person.id === "abc") && (Person.age === 18)

        assertTrue(
          kce == CompositePrimaryKeyExpr(
            PartitionKeyEquals(PartitionKey("id"), value = AttributeValue.String("abc")),
            SortKeyEquals(SortKey("age"), value = AttributeValue.Number(BigDecimal.valueOf(18)))
          )
        )
      }
    ),
    suite("automatically convert SchemaExpr to an UpdateExpression")(
      test("Person.age.add(1)") {
        val ue: UpdateExpression.Action.AddAction[Person] = Person.age.add(1)

        assertTrue(
          ue ==
            UpdateExpression.Action.AddAction[Person](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
              AttributeValue.Number(BigDecimal.valueOf(1))
            )
        )
      },
      test("Person.age.remove") {
        val ue: UpdateExpression.Action.RemoveAction[Person] = Person.age.remove

        assertTrue(
          ue ==
            UpdateExpression.Action.RemoveAction[Person](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age")
            )
        )
      },
      test("Person.list[0].remove") {
        val ue: Action.RemoveAction[Person] = Person.atList(0).remove[Person]

        assertTrue(
          ue ==
            UpdateExpression.Action.RemoveAction[Person](
              ProjectionExpression.ListElement(
                parent = ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "list"),
                index = 0
              )
            )
        )
      },
      test("Person.age.set(42)") {
        val ue: UpdateExpression.Action.SetAction[Person, Int] = Person.age.set(42)

        assertTrue(
          ue ==
            UpdateExpression.Action.SetAction[Person, Int](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
              UpdateExpression.SetOperand.ValueOperand(AttributeValue.Number(BigDecimal.valueOf(42)))
            )
        )
      },
      test("UpdateExpression conjunction - Person.age.set(42) + Person.age.set(42)") {
        val ue: UpdateExpression.Action[Person] = Person.age.set(42) + Person.age.set(42)

        assertTrue(
          ue ==
            UpdateExpression.Action.Actions[Person](
              zio.Chunk(
                UpdateExpression.Action.SetAction[Person, Int](
                  ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
                  UpdateExpression.SetOperand.ValueOperand(AttributeValue.Number(BigDecimal.valueOf(42)))
                ),
                UpdateExpression.Action.SetAction[Person, Int](
                  ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
                  UpdateExpression.SetOperand.ValueOperand(AttributeValue.Number(BigDecimal.valueOf(42)))
                )
              )
            )
        )
      },
      test("Person.age.setIfNotExists(42)") {
        val ue: UpdateExpression.Action.SetAction[Person, Int] = Person.age.setIfNotExists(42)

        assertTrue(
          ue ==
            UpdateExpression.Action.SetAction[Person, Int](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
              UpdateExpression.SetOperand.IfNotExists(
                ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "age"),
                AttributeValue.Number(BigDecimal.valueOf(42))
              )
            )
        )
      }
    )
  )

  private def rootMapElement[A](key: String): ProjectionExpression.MapElement[A, ProjectionExpression.Unknown] =
    ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key)

  private def numberValueOperand[A](n: BigDecimal): ConditionExpression.Operand[A, _] =
    Operand.ValueOperand(AttributeValue.Number(n))
}
