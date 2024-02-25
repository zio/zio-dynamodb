package example

import zio.dynamodb.json._
import zio.schema.DeriveSchema

object Example extends App {
  final case class Person(name: String, age: Int)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
  }

  val person = Person("Bob", 42)
  val json   = person.toJsonString

  println(json)
}
