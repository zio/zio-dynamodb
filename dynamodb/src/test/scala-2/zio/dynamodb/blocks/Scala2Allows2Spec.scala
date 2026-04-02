package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Optional, Schema }
import zio.dynamodb._
import zio.dynamodb.UpdateExpression.SetOperand.{ ListAppend, ListPrepend, PathOperand }
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

import java.time.Instant

object Scala2Allows2Spec extends ZIOSpecDefault {
  // TODO: Manually wrapped types

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

  case class Address(number: String, postcode: String)
  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived
    val number: Optic[Address, String]   = $(_.number)
    val postcode: Optic[Address, String] = $(_.postcode)
  }

  case class Person(
    id: PersonId,
    name: String,
    age: Int,
    count: Int,
    tupleMixed: (String, Int, Address),
    ageNewtype: Age,
    setInt: Set[Int] = Set.empty,
    setString: Set[String] = Set.empty,
    setPersonId: Set[PersonId] = Set.empty,
    setInstant: Set[Instant] = Set.empty,
    map: Map[String, Int] = Map.empty,
    mapOfAddress: Map[String, Address] = Map.empty,
    listInt: List[Int] = Nil,
    listAddress: List[Address] = Nil,
    binary: List[Byte] = Nil,
    binarySet: Set[List[Byte]] = Set.empty
  )
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]                        = Schema.derived
    val id: Optic[Person, PersonId]                            = $(_.id)
    val name: Optic[Person, String]                            = $(_.name)
    val age: Optic[Person, Int]                                = $(_.age)
    val count: Optic[Person, Int]                              = $(_.count)
    val tupleMixed: Optic[Person, (String, Int, Address)]      = $(_.tupleMixed)
    val ageNewtype: Optic[Person, Age]                         = $(_.ageNewtype)
    val setInt: Optic[Person, Set[Int]]                        = $(_.setInt)
    val setString: Optic[Person, Set[String]]                  = $(_.setString)
    val setPersonId: Optic[Person, Set[PersonId]]              = $(_.setPersonId)
    val setInstant: Optic[Person, Set[Instant]]                = $(_.setInstant)
    val map: Optic[Person, Map[String, Int]]                   = $(_.map)
    val mapOfAddress: Optic[Person, Map[String, Address]]      = $(_.mapOfAddress)
    def mapOfAddressAt(key: String): Optional[Person, Address] = $(_.mapOfAddress.atKey(key))
    val listInt: Optic[Person, List[Int]]                      = $(_.listInt)
    def listIntAt(index: Int): Optional[Person, Int]           = $(_.listInt.at(index))
    val listAddress: Optic[Person, List[Address]]              = $(_.listAddress)
    val binary: Optic[Person, List[Byte]]                      = $(_.binary)
    val binarySet: Optic[Person, Set[List[Byte]]]              = $(_.binarySet)
  }

  import ExtensionMethods._

  override def spec =
    suite("Allows syntax experiments")(
      test("using extension methods") {
        Person.id.add(PersonId(1))
        Person.id.between(PersonId(1), PersonId(3))
        Person.id.inSet(Set(PersonId(1), PersonId(2)))
        Person.age.add(1)
        Person.age.between(18, 21)
        Person.age.set(21)
        Person.age.set(Person.count)
        Person.ageNewtype.add(1.0)
        Person.setInt.addSet(Set(1))
        Person.setInt.deleteFromSet(Set(1))
        Person.listInt.prependList(List(1, 2))
        Person.listIntAt(1).set(21)
//        Person.setInt.between(1, 10)
//        Person.setInt.remove(1)
        Person.setString.addSet(Set("hello"))
        Person.setString.deleteFromSet(Set("hello"))
//        Person.setString.contains(1)
        Person.setPersonId.addSet(Set(PersonId(1)))
        Person.listInt.remove(1)
        Person.listInt.contains(1) // Should use IsNominalType
        Person.listInt.appendList(List(1, 2))

        Person.listAddress.remove(1)
        Person.setInt.addSet(Set(1))
//        Person.setPersonId.contains(PersonId(1))

        Person.mapOfAddressAt("42").remove
        Person.name.contains("1")
        Person.name.between("A", "Z")
        Person.name.inSet(Set("Alice", "Bob"))
        Person.binary.between(List(Byte.MinValue), List(Byte.MaxValue))

        assertTrue(true)
      },
      test("add age wrapper type") {
        val x: UpdateExpression.Action.AddAction[Person] = Person.ageNewtype.add(Age(21))

        assertTrue(
          x == UpdateExpression.Action
            .AddAction[Person](
              ProjectionExpression.MapElement(
                ProjectionExpression.Root,
                "ageNewtype"
              ),
              AttributeValue.Number(BigDecimal.valueOf(21))
            )
        )
      }
    )

  object ExtensionMethods {

    import zio.blocks.schema.comptime.Allows
    import Allows._
    import zio.blocks.typeid.IsNominalType

    // scalars
    type N    =
      Primitive.Int `|` Primitive.Long `|` Primitive.Float `|` Primitive.Double `|` Primitive.Short `|` Wrapped[Self]
    type S    = Primitive.String `|` Wrapped[Self]
    type BOOL = Primitive.Boolean
    type B    = Sequence[Primitive.Byte] `|` Wrapped[Self]
    // I think we can ignore NULL for incoming Scala types

    type NS = Sequence.Set[N `|` Wrapped[N]]
    type SS = Sequence.Set[S `|` Wrapped[S]]
    type BS = Sequence.Set[B]

    // list excludes Sets - note we need to explicitly add Record here for List[Address]
    type L = Sequence.List[All `|` Record[All]] `|` Sequence.Vector[All `|` Record[All]] `|` Sequence.Array[
      All `|` Record[All]
    ] `|`
      Sequence.Chunk[All | Record[All]]

    type M = Map[Primitive.String, All]

    // single recursive root
    type All =
      N `|` S `|` BOOL `|` B `|` NS `|` SS `|` BS `|` Record[Self] `|` Sequence[Self] `|` Map[Self, Self]

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
        ev: Allows[A, N `|` Wrapped[N]],
        ev2: Allows[To, N `|` Wrapped[N]],
        to: ToAttributeValue[A]
      ): UpdateExpression.Action.AddAction[From] = {
        val (_, _) = (ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.AddAction(
          self,
          to.toAttributeValue(a)
        )
      }

      /** Only applies to a List */
      def appendList[A](
        xs: To
      )(implicit
        ev: Allows[To, L],
//        ev2: Allows[To, Sequence[IsType[A]]],
        ev3: To <:< Iterable[A],
        to: ToAttributeValue[A]
      ): UpdateExpression.Action.SetAction[From, To] = {
        val (_, _) = (ev, ev3) // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.SetAction(
          self,
          ListAppend(
            self,
            AttributeValue.List(xs.toList.map(to.toAttributeValue))
          )
        )
      }

      def addSet[A](
        set: Set[A]
      )(implicit
        ev: Allows[To, NS `|` SS `|` BS],
        evSet: Set[A] <:< To
      ): UpdateExpression.Action.AddAction[From] = {
        val _ = ev // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.AddAction(
          self,
          ToAttributeValue[To].toAttributeValue(evSet(set))
        )
      }

      /** valid for N | S | B */
      def between(
        minValue: To,
        maxValue: To
      )(implicit ex: Allows[To, N `|` S `|` B]): ConditionExpression[From] = {
        val _ = ex // to silence unused warnings - we just need the evidence for the compiler
        ConditionExpression.Operand
          .ProjectionExpressionOperand(self)
          .between(
            ToAttributeValue[To].toAttributeValue(minValue),
            ToAttributeValue[To].toAttributeValue(maxValue)
          )
      }

      def contains[A: IsNominalType](a: A)(implicit
        ev: Allows[To, NS `|` SS `|` BS `|` L],
        ev2: Allows[To, Sequence[IsType[A]]],
        to: ToAttributeValue[A]
      ): ConditionExpression[From] = {
        val (_, _) = (ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
        ConditionExpression.Contains(self, to.toAttributeValue(a))
      }

      def contains(a: String)(implicit
        ev: Allows[To, S]
      ): ConditionExpression[From] = {
        val _ = ev // to silence unused warnings - we just need the evidence for the compiler
        ConditionExpression.Contains(self, AttributeValue.String(a))
      }

      def deleteFromSet(
        set: To
      )(implicit
        ev: Allows[To, NS `|` SS `|` BS],
        to: ToAttributeValue[To]
      ): UpdateExpression.Action.DeleteAction[From] = {
        val _ = ev // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.DeleteAction(
          self,
          to.toAttributeValue(set)
        )
      }

      def prependList[A](
        xs: To
      )(implicit
        ev: Allows[To, L],
        ev2: To <:< Iterable[A],
        to: ToAttributeValue[A]
      ): UpdateExpression.Action.SetAction[From, To] = {
        val (_, _) = (ev, ev2) // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.SetAction(
          self,
          ListPrepend(
            self,
            AttributeValue.List(xs.toList.map(to.toAttributeValue))
          )
        )
      }

      /** Attribute must be a scalar ie N | S | B */
      def inSet(
        values: Set[To]
      )(implicit ev: Allows[To, N `|` S `|` B]): ConditionExpression[From] = {
        val _ = ev // to silence unused warnings - we just need the evidence for the compiler
        ConditionExpression.Operand
          .ProjectionExpressionOperand(self)
          .in(values.map(ToAttributeValue[To].toAttributeValue))
      }

      // TODO: prepend - only valid for a L attribute

      /**
       * Removes this PathExpression from an item - always valid as we have a valid path via an optic in hand
       */
      def remove: UpdateExpression.Action.RemoveAction[From] =
        UpdateExpression.Action.RemoveAction[From](self)

      /*
  Remove at index UpdateExpression behaviour
  | Attribute Type | Allowed? |
  | -------------- | -------- |
  | `L` (List)     | ✅       |
  | `SS`           | ❌       |
  | `NS`           | ❌        |
  | `BS`           | ❌        |
  | `N`            | ❌        |
  | `S`            | ❌        |
  | `M`            | ❌        |
       */
      def remove(
        index: Int // we need extra constraint to exclude Sets etc: evSeq: To <:< Seq[_]
      )(implicit ev: Allows[To, L]): UpdateExpression.Action.RemoveAction[From] = {
        val _ = ev // to silence unused warnings - we just need the evidence for the compiler
        UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))
      }

      def set(
        a: To
      ): UpdateExpression.Action.SetAction[From, To] =
        UpdateExpression.Action.SetAction(
          self,
          UpdateExpression.SetOperand.ValueOperand(
            ToAttributeValue[To].toAttributeValue(a)
          )
        )

      def set(
        o: Optic[From, To]
      ): UpdateExpression.Action.SetAction[From, To] = {
        val oAsPE = OpticToPE.pe(o)
        UpdateExpression.Action.SetAction(self, PathOperand(oAsPE))
      }

    }

  }
}
