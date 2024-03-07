package zio.dynamodb.json

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import zio.schema.DeriveSchema
import zio.dynamodb.DynamoDBError
import zio.dynamodb.AttrMap
import zio.schema.annotation.discriminatorName
import zio.dynamodb.json.fromItem

object SyntaxSpec extends ZIOSpecDefault {
  @discriminatorName("invoiceType")
  sealed trait Invoice
  object Invoice {
    final case class PreBilled(id: String, sku: String) extends Invoice
    object PreBilled {
      implicit val schema = DeriveSchema.gen[PreBilled]
    }
    final case class Billed(id: String, sku: String, cost: Int) extends Invoice
    object Billed    {
      implicit val schema = DeriveSchema.gen[Billed]
    }
    implicit val schema = DeriveSchema.gen[Invoice]
  }

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

  val sumTypeSuite = suite("Sum type suite")(
    test("encode with top level sum type renders discriminator") {
      val preBilled  = Invoice.PreBilled("id", "sku")
      val jsonString = preBilled.toJsonString[Invoice]
      assertTrue(jsonString == """{"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}""")
    },
    test("encode with concrete type") {
      val preBilled  = Invoice.PreBilled("id", "sku")
      val jsonString = preBilled.toJsonString
      assertTrue(jsonString == """{"sku":{"S":"sku"},"id":{"S":"id"}}""")
    },
    test("decode with top level sum type") {
      val jsonString = """{"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}"""
      val item       = parseItem(jsonString).flatMap(fromItem[Invoice])
      assertTrue(item == Right(Invoice.PreBilled("id", "sku")))
    }
  )

  val spec = suite("DynamodbJsonCodecSpec")(sumTypeSuite)
}
