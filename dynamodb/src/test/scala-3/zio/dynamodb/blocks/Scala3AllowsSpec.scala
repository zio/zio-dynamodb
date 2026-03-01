package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Schema }
import zio.dynamodb.proofs.{ Addable, ListRemoveable }
import zio.dynamodb.{ blocks, ProjectionExpression, ProjectionExpressionOps, ToAttributeValue, UpdateExpression }
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
    set: Set[Int] = Set.empty,
    map: Map[String, Int] = Map.empty,
    list: List[Int] = Nil
  )
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]      = Schema.derived
    val id: Optic[Person, PersonId]          = $(_.id)
    val name: Optic[Person, String]          = $(_.name)
    val age: Optic[Person, Int]              = $(_.age)
    val ageWrapped: Optic[Person, Age]       = $(_.ageWrapped)
    val set: Optic[Person, Set[Int]]         = $(_.set)
    val map: Optic[Person, Map[String, Int]] = $(_.map)
    val list: Optic[Person, List[Int]]       = $(_.list)
  }

  override def spec =
    suite("Scala 3 allows syntax")(
      test("using Scala 3 extension methods syntax") {
        import ExtensionMethods._
        Person.id.add(PersonId(1))
        Person.age.add2(1)
        Person.ageWrapped.add2(1.0)
        Person.set.addSetOld(Set(1))
        Person.list.remove(1)

        assertTrue(true)
      }
    )
}
object ExtensionMethods {
  import zio.blocks.schema.comptime.Allows
  import Allows._

  type Numeric        = Primitive.Int | Primitive.Long | Primitive.Float | Primitive.Double | Primitive.Short
  type WrappedNumeric = Wrapped[Numeric]
  type Addable2       = Sequence[Primitive.Int] //Record[Primitive.Int]

  implicit class OpticToDdbExpr[From, To: ToAttributeValue](optic: Optic[From, To]) {
    private def self: ProjectionExpression[From, To] = OpticToPE.pe(optic)

    def add(a: To)(implicit
      ev: Allows[To, Numeric | Wrapped[Numeric]],
      to: ToAttributeValue[To]
    ): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(
        self,
        to.toAttributeValue(a)
      )

    def add2[A](a: A)(implicit
      ev: Allows[A, Numeric],
      ev2: Allows[To, Numeric | Wrapped[Numeric]],
      to: ToAttributeValue[A]
    ): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(
        self,
        to.toAttributeValue(a)
      )

//    def addSet[A](
//      set: Set[A]
//    )(implicit
//      ev: Allows[To, Sequence[Primitive.Int]],
//      to: ToAttributeValue[A],
//      evSet: Set[A] <:< To
//    ): UpdateExpression.Action.AddAction[From] =
//      UpdateExpression.Action.AddAction(
//        self,
//        to.toAttributeValue(evSet(set))
//      )

    def addSetOld[A](
      set: Set[A]
    )(implicit ev: Addable[To, A], evSet: Set[A] <:< To): UpdateExpression.Action.AddAction[From] = {
      val (_, _) = (ev, evSet)
      UpdateExpression.Action.AddAction(
        self,
        ToAttributeValue[To].toAttributeValue(evSet(set))
      )
    }

    def remove(
      index: Int
    )(implicit ev: Allows[To, Sequence[Primitive.Int]]): UpdateExpression.Action.RemoveAction[From] =
      UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))

    def removeOld[From2 <: From](
      index: Int
    )(implicit ev: ListRemoveable[To]): UpdateExpression.Action.RemoveAction[From2] =
      UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
  }
}
