package example

import zio.dynamodb.{ AttrMap, DynamoDBError }
import zio.dynamodb.json._
import zio.schema.DeriveSchema

object Example extends App {
  final case class Person(name: String, age: Int)
  object Person {
    implicit val schema = DeriveSchema.gen[Person]
  }

  val person = Person("Bob", 42)
  val json   = person.toJsonString

  println(json) // {"age":{"N":"42"},"name":{"S":"Bob"}}

  val item: Either[DynamoDBError.ItemError, AttrMap] = parseItem("""{"age":{"N":"42"},"name":{"S":"Bob"}}""")
  println(item) // Right(AttrMap(Map(name -> S(Bob), age -> N(42))))

  val person2 = item.map(fromItem[Person])
  println(person2) // Right(Right(Person(Bob,42)))
}
