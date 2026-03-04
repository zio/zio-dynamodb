package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Optional, Schema }
import zio.dynamodb.proofs.{ Addable, ListRemoveable }
import zio.dynamodb.{ blocks, AttributeValue, ProjectionExpression, ToAttributeValue, UpdateExpression }
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

object Scala3AllowsSpec extends ZIOSpecDefault {
  opaque type OpaqueId = Int
  object OpaqueId {
    def apply(value: Int): OpaqueId = value
  }
  extension (id: OpaqueId) {
    def value: Int = id
  }

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
    tupleMixed: (String, Int, Address),
    opaqueInt: OpaqueId,
    ageNewtype: Age,
    setInt: Set[Int] = Set.empty,
    setString: Set[String] = Set.empty,
    setPersonId: Set[PersonId] = Set.empty,
    map: Map[String, Int] = Map.empty,
    mapOfAddress: Map[String, Address] = Map.empty,
    listInt: List[Int] = Nil,
    listAddress: List[Address] = Nil
  )
  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]                        = Schema.derived
    val id: Optic[Person, PersonId]                            = $(_.id)
    val name: Optic[Person, String]                            = $(_.name)
    val age: Optic[Person, Int]                                = $(_.age)
    val tupleMixed: Optic[Person, (String, Int, Address)]      = $(_.tupleMixed)
    val opaqueInt: Optic[Person, OpaqueId]                     = $(_.opaqueInt)
    val ageNewtype: Optic[Person, Age]                         = $(_.ageNewtype)
    val setInt: Optic[Person, Set[Int]]                        = $(_.setInt)
    val setString: Optic[Person, Set[String]]                  = $(_.setString)
    val setPersonId: Optic[Person, Set[PersonId]]              = $(_.setPersonId)
    val map: Optic[Person, Map[String, Int]]                   = $(_.map)
    val mapOfAddress: Optic[Person, Map[String, Address]]      = $(_.mapOfAddress)
    def mapOfAddressAt(key: String): Optional[Person, Address] = $(_.mapOfAddress.atKey(key))
    val listInt: Optic[Person, List[Int]]                      = $(_.listInt)
    val listAddress: Optic[Person, List[Address]]              = $(_.listAddress)
  }

  import ExtensionMethods._

  override def spec =
    suite("Allows syntax experiments")(
      test("using extension methods") {
        Person.id.add(PersonId(1))
        Person.age.add(1)
        Person.opaqueInt.add(OpaqueId(1))
        Person.ageNewtype.add(1.0)
        Person.setInt.addSet(Set(1))
        Person.setString.addSet(Set("hello"))
        Person.setPersonId.addSet(Set(PersonId(1)))
        Person.listInt.remove(1)
        Person.listAddress.remove(1)
        Person.setInt.addSet(Set(1))
        Person.setPersonId.contains(PersonId(1))

        assertTrue(true)
      },
      test("add age wrapper type") {
        val x = Person.ageNewtype.add(Age(21))

        assertTrue(
          x == UpdateExpression.Action
            .AddAction(
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

    // scalars
    type N    = Primitive.Int | Primitive.Long | Primitive.Float | Primitive.Double | Primitive.Short
    type S    = Primitive.String
    type BOOL = Primitive.Boolean
    // I think we can ignore NULL for incomming Scala types

    // sets - approximate a Set using Sequence for now
    type NS = Sequence[N | Wrapped[N]]
    type SS = Sequence[S | Wrapped[S]]
    type BS = Sequence[Sequence[Primitive.Byte] | Wrapped[Sequence[Primitive.Byte]]]

    // recursive containers
    type L = Sequence[All | Record[All]] // need to explicitly add Record here for List[Address ]
    type M = Map[Primitive.String, All]

    // single recursive root
    type All =
      N | S | BOOL | NS | SS | BS | Record[Self] | Sequence[Self] | Map[Self, Self]

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
        ev: Allows[A, N | Wrapped[N]],
        ev2: Allows[To, N | Wrapped[N]],
        to: ToAttributeValue[A]
      ): UpdateExpression.Action.AddAction[From] =
        UpdateExpression.Action.AddAction(
          self,
          to.toAttributeValue(a)
        )

      def addSet[A](
        set: Set[A]
      )(implicit
        ev: Allows[To, NS | SS | BS],
        evSet: Set[A] <:< To
      ): UpdateExpression.Action.AddAction[From] =
        UpdateExpression.Action.AddAction(
          self,
          ToAttributeValue[To].toAttributeValue(evSet(set))
        )

      def contains[A](
                       a: A
                     )(implicit
                       ev: Allows[To, NS | SS | BS | L | S],
                       ev2: Containable[To, A],
                       to: ToAttributeValue[A]
                     ): ConditionExpression[From] =
        ConditionExpression.Contains(self, to.toAttributeValue(a))

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
        index: Int
      )(implicit ev: Allows[To, L]): UpdateExpression.Action.RemoveAction[From] =
        UpdateExpression.Action.RemoveAction(ProjectionExpression.ListElement(self, index))

    }

    sealed trait Containable[X, -A]

    object Containable {
      implicit def set[A]: Containable[Set[A], A] = new Containable[Set[A], A] {}

      implicit def list[A]: Containable[List[A], A] = new Containable[List[A], A] {}

      implicit def string: Containable[String, String] = new Containable[String, String] {}
    }
  }
}
