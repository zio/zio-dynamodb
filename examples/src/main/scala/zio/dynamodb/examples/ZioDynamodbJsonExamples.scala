package zio.dynamodb.examples

import zio.dynamodb.json._
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName

import scala.annotation.nowarn

object ZioDynamodbJsonExample extends App {
  @discriminatorName("invoiceType")
  sealed trait Invoice
  object Invoice {
    final case class PreBilled(id: String, sku: String) extends Invoice
    object PreBilled {
      @nowarn
      implicit val schema = DeriveSchema.gen[PreBilled]
    }
    final case class Billed(id: String, sku: String, cost: Int) extends Invoice
    object Billed    {
      @nowarn
      implicit val schema = DeriveSchema.gen[Billed]
    }
    @nowarn
    implicit val schema = DeriveSchema.gen[Invoice]
  }

  // get the rendered json string from a case class
  val preBilled  = Invoice.PreBilled("id", "sku")
  val jsonString = preBilled.toJsonString[Invoice]
  println(jsonString) // {"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}

  // decode the json string to a case class
  val errorOrInvoice = parse[Invoice](jsonString)
  println(errorOrInvoice) // Right(Invoice.PreBilled("id", "sku")

  // decode the json string to an Item
  val errorOrItem = parseItem(jsonString)
  println(errorOrItem) // Right(AttrMap(Map("sku" -> S("sku"), "id" -> S("id"), "invoiceType" -> S("PreBilled"))))

  // get the rendered json string from an Item
  errorOrItem
    .map(item => item.toJsonString)
    .map(println) // {"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}

}
