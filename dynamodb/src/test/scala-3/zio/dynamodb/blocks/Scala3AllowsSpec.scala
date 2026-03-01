package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Schema }
import zio.dynamodb.proofs.{ Addable, ListRemoveable }
import zio.dynamodb.{
  blocks,
  AttributeValue,
  ProjectionExpression,
  ProjectionExpressionOps,
  ToAttributeValue,
  UpdateExpression
}
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

object Scala3AllowsSpec extends ZIOSpecDefault {
  object PersonId extends Newtype[Int] {
    implicit val x: Schema[PersonId.Type] =
      Schema[Int].transform(s => PersonId(s), (personId: PersonId) => PersonId.unwrap(personId))
  }
  type PersonId = PersonId.Type

  object Age extends Newtype[Int] {
    implicit val x: Schema[Age.Type] =
      Schema[Int].transform(s => Age(s), (age: Age) => Age.unwrap(age))
  }
  type Age = Age.Type

  case class Person(
    id: PersonId,
    name: String,
    age: Int,
    ageWrapped: Age,
    setInt: Set[Int] = Set.empty,
    setString: Set[String] = Set.empty,
    setPersonId: Set[PersonId] = Set.empty,
    map: Map[String, Int] = Map.empty,
    list: List[Int] = Nil
  )
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]           = Schema.derived
    val id: Optic[Person, PersonId]               = $(_.id)
    val name: Optic[Person, String]               = $(_.name)
    val age: Optic[Person, Int]                   = $(_.age)
    val ageWrapped: Optic[Person, Age]            = $(_.ageWrapped)
    val setInt: Optic[Person, Set[Int]]           = $(_.setInt)
    val setString: Optic[Person, Set[String]]     = $(_.setString)
    val setPersonId: Optic[Person, Set[PersonId]] = $(_.setPersonId)
    val map: Optic[Person, Map[String, Int]]      = $(_.map)
    val list: Optic[Person, List[Int]]            = $(_.list)
  }

  import ExtensionMethods._

  override def spec =
    suite("Allows syntax experiments")(
      test("using extension methods") {
        Person.id.add(PersonId(1))
        Person.age.add(1)
        Person.ageWrapped.add(1.0)
        Person.setInt.addSet(Set(1))
        Person.setString.addSet(Set("hello"))
        Person.setPersonId.addSet(Set(PersonId(1)))
        Person.list.remove(1)
        Person.setInt.addSet(Set(1))

        assertTrue(true)
      },
      test("add age wrapper type") {
        val x = Person.ageWrapped.add(Age(21))

        assertTrue(
          x == UpdateExpression.Action
            .AddAction(
              ProjectionExpression.MapElement(
                ProjectionExpression.Root,
                "ageWrapped"
              ),
              AttributeValue.Number(BigDecimal.valueOf(21))
            )
        )
      }
    )
}
object ExtensionMethods {
  import zio.blocks.schema.comptime.Allows
  import Allows._

  type Numeric = Primitive.Int | Primitive.Long | Primitive.Float | Primitive.Double | Primitive.Short

  // we have no Set so use Sequence as an approximation for now
  type NumberSet = Sequence[Numeric] | Sequence[Wrapped[Numeric]]
  type StringSet = Sequence[Primitive.String] | Sequence[Wrapped[Primitive.String]]
  type BinarySet = Sequence[Sequence[Primitive.Byte]]
  type NativeSet = NumberSet | StringSet | BinarySet

  implicit class OpticToDdbExpr[From, To: ToAttributeValue](optic: Optic[From, To]) {
    private def self: ProjectionExpression[From, To] = OpticToPE.pe(optic)

    /*
ADD update behaviour
| Attribute Type    | Allowed? | Behaviour         |
| ----------------- | -------- | ----------------- |
| `N` (Number)      | ✅        | Numeric increment |
| `NS` (Number Set) | ✅        | Set union         |
| `SS` (String Set) | ✅        | Set union         |
| `BS` (Binary Set) | ✅        | Set union         |
| `S` (String)      | ❌        | Not allowed       |
| `L` (List)        | ❌        | Not allowed       |
| `M` (Map)         | ❌        | Not allowed       |
| `BOOL`            | ❌        | Not allowed       |
| `NULL`            | ❌        | Not allowed       |
     */

    def add[A](a: A)(implicit
      ev: Allows[A, Numeric | Wrapped[Numeric]],
      ev2: Allows[To, Numeric | Wrapped[Numeric]],
      to: ToAttributeValue[A]
    ): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(
        self,
        to.toAttributeValue(a)
      )

    def addSet[A](
      set: Set[A]
    )(implicit
      ev: Allows[To, NativeSet],
      evSet: Set[A] <:< To
    ): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(
        self,
        ToAttributeValue[To].toAttributeValue(evSet(set))
      )

    /*
Remove at index UpdateExpression behaviour
| Attribute Type | Allowed? |
| -------------- | -------- |
| `L` (List)     | ✅        |
| `SS`           | ❌        |
| `NS`           | ❌        |
| `BS`           | ❌        |
| `N`            | ❌        |
| `S`            | ❌        |
| `M`            | ❌        |
     */
    def remove(
      index: Int
    )(implicit ev: Allows[To, Sequence[Numeric]]): UpdateExpression.Action.RemoveAction[From] =
      UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))

    def removeOld[From2 <: From](
      index: Int
    )(implicit ev: ListRemoveable[To]): UpdateExpression.Action.RemoveAction[From2] =
      UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
  }
}
