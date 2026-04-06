package zio.dynamodb.blocks

import zio.blocks.schema.{ CompanionOptics, Optic, Optional, Schema }
import zio.dynamodb.proofs.{ Addable, ListRemoveable }
import zio.dynamodb.*
import zio.dynamodb.UpdateExpression.SetOperand.{ ListAppend, ListPrepend, PathOperand }
import zio.prelude.Newtype
import zio.test.{ assertTrue, ZIOSpecDefault }

import java.time.Instant

object Scala3AllowsSpec extends ZIOSpecDefault {
  // TODO: Manually wrapped types

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
    count: Int,
    tupleMixed: (String, Int, Address),
    opaqueInt: OpaqueId,
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
    val opaqueInt: Optic[Person, OpaqueId]                     = $(_.opaqueInt)
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

  import ExtensionMethods.*

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
        Person.opaqueInt.add(OpaqueId(1))
        Person.opaqueInt.between(18, 21)
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
        Person.listInt.contains(1)
        Person.listInt.appendList(List(1, 2))

        Person.listAddress.remove(1)
        Person.setInt.addSet(Set(1))
//        Person.setInt.appendList(List(1, 2))
        Person.setPersonId.contains(PersonId(1))

        Person.mapOfAddressAt("42").remove
        Person.name.contains("1")
        Person.name.between("A", "Z")
        Person.name.inSet(Set("Alice", "Bob"))
        Person.binary.between(List(Byte.MinValue), List(Byte.MaxValue))

//        Person.setInstant.deleteFromSet(Set.empty)

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

}
