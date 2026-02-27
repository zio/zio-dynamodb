package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Lens, Optional, Schema, SchemaExpr }
import zio.dynamodb.ConditionExpression.Operand
import zio.dynamodb.DynamoDBQuery.PutItem
import zio.dynamodb.KeyConditionExpr.{
  CompositePrimaryKeyExpr,
  ExtendedCompositePrimaryKeyExpr,
  PartitionKeyEquals,
  SortKeyEquals
}
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb._
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

object SchemaExprConversionSpec extends ZIOSpecDefault {
  import BlocksApi._

  final case class Person(id: String, age: Int, list: List[String], map: Map[String, Int], set: Set[Int] = Set.empty)
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person] = Schema.derived

    val id: Lens[Person, String]                     = $(_.id)
    val age: Lens[Person, Int]                       = $(_.age)
    val list: Lens[Person, List[String]]             = $(_.list)
    def listAt(i: Int): Optional[Person, String]     = $(_.list.at(i))
    def mapAtKey(key: String): Optional[Person, Int] = $(_.map.atKey(key))
    val set: Lens[Person, Set[Int]]                  = $(_.set)
  }

  object PersonId extends Newtype[String] {
    implicit val x: Schema[blocks.SchemaExprConversionSpec.PersonId.Type] =
      Schema[String].transform(s => PersonId(s), (personId: PersonId) => PersonId.unwrap(personId))
  }
  type PersonId = PersonId.Type

  final case class PersonWithPreludeNewtype(personId: PersonId, age: Int)
  object PersonWithPreludeNewtype extends CompanionOptics[PersonWithPreludeNewtype] {
    implicit val schema: Schema[PersonWithPreludeNewtype]  = Schema.derived
    val personId: Lens[PersonWithPreludeNewtype, PersonId] = $(_.personId)
    val age: Lens[PersonWithPreludeNewtype, Int]           = $(_.age)
  }

  val putQuery = BlocksApi.put("table", Person("1", 30, List.empty, Map.empty))

  val spec = suite("SchemaExprSpec should")(
    suite("convert SchemaExpr to a ConditionExpression")(
      suite("conjunction")(
        test("Person.age > 18 && Person.age < 65") {
          val ce = extractCE(putQuery.where(Person.age > 18 && Person.age < 65)).get

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
          val ce = extractCE(putQuery.where(Person.age < 18 || Person.age > 65)).get

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
          val ce = extractCE(putQuery.where(Person.age > 18)).get

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
          val ce = extractCE(putQuery.where(Person.age >= 18)).get

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
          val ce = extractCE(putQuery.where(Person.age === 18)).get

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
          val ce = extractCE(putQuery.where(Person.age != 18)).get

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
          val ce = extractCE(putQuery.where(Person.age < 18)).get

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
          val ce = extractCE(putQuery.where(Person.age <= 18)).get

          assertTrue(
            ce ==
              ConditionExpression
                .LessThanOrEqual(
                  ConditionExpression.Operand.ProjectionExpressionOperand(rootMapElement("age")),
                  numberValueOperand(BigDecimal.valueOf(18))
                )
          )
        }
      ),
      test("Person.set.contains(1)") {
        val ce: ConditionExpression[Person] = Person.set.contains(1)

        assertTrue(
          ce ==
            ConditionExpression.Contains(
              ProjectionExpression.MapElement(
                parent = ProjectionExpression.Root,
                key = "set"
              ),
              AttributeValue.Number(BigDecimal.valueOf(1))
            )
        )
      },
      test("Person.set.containsSet(1, Set(2)") {
        val ce: ConditionExpression[Person] = Person.set.containsSet(1, Set(2))

        assertTrue(
          ce ==
            ConditionExpression.And(
              left = ConditionExpression.Contains(
                ProjectionExpression.MapElement(
                  parent = ProjectionExpression.Root,
                  key = "set"
                ),
                AttributeValue.Number(BigDecimal.valueOf(1))
              ),
              right = ConditionExpression.Contains(
                ProjectionExpression.MapElement(
                  parent = ProjectionExpression.Root,
                  key = "set"
                ),
                AttributeValue.Number(BigDecimal.valueOf(2))
              )
            )
        )
      }
    ), // end ConditionExpression suite
    suite("automatically convert SchemaExpr to a KeyCondition")(
      test("Person.id === 'abc'") {
        val schemaExpr: SchemaExpr[Person, Boolean] = Person.id === "abc"
        val kce: KeyConditionExpr[Person]           = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(schemaExpr)

        assertTrue(
          kce == PartitionKeyEquals(PartitionKey("id"), AttributeValue.String("abc"))
        )
      },
      test("PersonWithPreludeNewType.personId === 'abc'") {

        val schemaExpr: SchemaExpr[PersonWithPreludeNewtype, Boolean] =
          PersonWithPreludeNewtype.personId === PersonId("abc")
        val kce: KeyConditionExpr[PersonWithPreludeNewtype]           = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(schemaExpr)

        assertTrue(
          kce == PartitionKeyEquals(PartitionKey("personId"), AttributeValue.String("abc"))
        )
      },
      test("Person.id === 'abc' && Person.age == 18") {
        val schemaExpr                    = Person.id === "abc" && Person.age === 18
        val kce: KeyConditionExpr[Person] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(schemaExpr)

        assertTrue(
          kce == CompositePrimaryKeyExpr(
            PartitionKeyEquals(PartitionKey("id"), value = AttributeValue.String("abc")),
            SortKeyEquals(SortKey("age"), value = AttributeValue.Number(BigDecimal.valueOf(18)))
          )
        )
      },
      test("Person.id === 'abc' && Person.age > 18") {
        val schemaExpr                    = Person.id === "abc" && Person.age > 18
        val kce: KeyConditionExpr[Person] = BlocksApi.schemaExprToKeyConditionExprUnsafe(schemaExpr)

        assertTrue(
          kce == ExtendedCompositePrimaryKeyExpr(
            PartitionKeyEquals(PartitionKey("id"), value = AttributeValue.String("abc")),
            KeyConditionExpr.ExtendedSortKeyExpr
              .GreaterThan(SortKey("age"), value = AttributeValue.Number(BigDecimal.valueOf(18)))
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
      test("Person.set.addSet(Set(1)) - native set addition") {
        val ue: UpdateExpression.Action.AddAction[Person] = Person.set.addSet(Set(1))

        assertTrue(
          ue ==
            UpdateExpression.Action.AddAction[Person](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "set"),
              AttributeValue.NumberSet(Set(BigDecimal.valueOf(1)))
            )
        )
      },
      test("Person.set.deleteFromSet(Set(1))") {
        val ue: UpdateExpression.Action.DeleteAction[Person] = Person.set.deleteFromSet(Set(1))

        assertTrue(
          ue ==
            UpdateExpression.Action.DeleteAction[Person](
              ProjectionExpression.MapElement(parent = ProjectionExpression.Root, key = "set"),
              AttributeValue.NumberSet(Set(BigDecimal.valueOf(1)))
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
      test("Person.listAt[0].remove") {
        val ue: Action.RemoveAction[Person] = Person.listAt(0).remove[Person]

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

  private def extractCE[In, Out](q: DynamoDBQuery[In, Out]): Option[ConditionExpression[_]] =
    q match {
      // DynamoDB queries created via the High Level API are wrapped in a Map
      case DynamoDBQuery.Map(PutItem(_, _, ce, _, _, _, _), _) => ce
      case _                                                   => None
    }

}
