package zio.dynamodb.examples

import zio.blocks.schema._

object BlocksExample {
  final case class Address(number: Int, street: String)
  object Address extends CompanionOptics[Address] {
    implicit val schema: Schema[Address] = Schema.derived
    val reflect: Reflect.Bound[Address]  = schema.reflect
    val number: Lens[Address, Int]       = optic(_.number)
    val street: Lens[Address, String]    = optic(_.street)
  }

  final case class Person(
    id: Long,
    name: String,
    age: Int,
    childrenAges: List[Int],
    addresses: List[Address],
    addressMap: Map[Int, Address] = Map.empty
  )

  object Person extends CompanionOptics[Person] {
    implicit val schema: Schema[Person]             = Schema.derived
    val reflect: Reflect.Bound[Person]              = schema.reflect
    val id: Lens[Person, Long]                      = optic(_.id)
    val name: Lens[Person, String]                  = optic(_.name)
    val age: Lens[Person, Int]                      = optic(_.age)
    val addresses: Lens[Person, List[Address]]      = optic(_.addresses)
    val childrenAges: Traversal[Person, Int]        = optic(_.childrenAges).listValues
    val addressMap: Lens[Person, Map[Int, Address]] = optic(_.addressMap)
  }

  def main(args: Array[String]): Unit = {
    val person = Person(
      12345678901L,
      "John",
      30,
      List(5, 7, 9),
      List(Address(123, "Main St")),
      Map(1 -> Address(456, "Second St"))
    )

    println("id:         " + Person.id.get(person))
    println("name:       " + Person.name.get(person))
    println("age:        " + Person.age.get(person))
    println("address:    " + Person.addresses.get(person))
    println("newPerson:  " + Person.name.replace(person, "Jane"))
    println("newPerson2: " + Person.childrenAges.modify(person, _ + 1))
    println("newPerson3: " + Person.childrenAges.modify(person, (i: Int) => if (i == 7) i + 1 else i))
    /*
    QUESTIONS
    - how can you compose a Lens to address a collection element by index?
    - how can you compose a Lens to address a Map element by key?
     */
  }
}
