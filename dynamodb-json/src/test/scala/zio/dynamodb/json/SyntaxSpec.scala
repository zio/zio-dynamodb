package zio.dynamodb.json

import zio.test.ZIOSpecDefault
import zio.test.assertTrue
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName

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

  val sumTypeSuite = suite("Sum type suite")(
    test("encode with top level sum type renders discriminator") {
      val preBilled  = Invoice.PreBilled("id", "sku")
      val jsonString = preBilled.toJsonString[Invoice]
      assertTrue(jsonString == """{"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}""")
    },
    test("encode with concrete type does not render discriminator") {
      val preBilled  = Invoice.PreBilled("id", "sku")
      val jsonString = preBilled.toJsonString
      assertTrue(jsonString == """{"sku":{"S":"sku"},"id":{"S":"id"}}""")
    },
    test("decode with top level sum type") {
      val jsonString     = """{"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}"""
      val errorOrInvoice = parse[Invoice](jsonString)
      assertTrue(errorOrInvoice == Right(Invoice.PreBilled("id", "sku")))
    },
    test("decode with concrete type") {
      val jsonString     = """{"sku":{"S":"sku"},"id":{"S":"id"}}"""
      val errorOrInvoice = parse[Invoice.PreBilled](jsonString)
      assertTrue(errorOrInvoice == Right(Invoice.PreBilled("id", "sku")))
    }
  )

  val spec = suite("DynamodbJsonCodecSpec")(sumTypeSuite)
}
