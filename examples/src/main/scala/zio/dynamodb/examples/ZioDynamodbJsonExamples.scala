package zio.dynamodb.examples

import zio.dynamodb.json._
import zio.dynamodb.AttrMap
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName

import zio.schema.Schema

/**
 * zio-dynamodb-json is a new experimental module designed for debugging and troubleshooting purposes - it should not be used in production code.
 * It works at the level of the AttributeValue type and so works with both the low level and high level APIs.
 * Note ATM it does not support the Binary and Binary Set types.
 *
 * Some example use cases include:
 * - visualizing the Attribute Value representation of a case class during model development
 * - production troubleshooting - grabbing DDB JSON from the AWS console in production and decoding it to a case class for debugging
 */
object ZioDynamodbJsonExample extends App {
  @discriminatorName("invoiceType")
  sealed trait Invoice
  object Invoice {
    final case class PreBilled(id: String, sku: String) extends Invoice
    object PreBilled {
      implicit val schema: Schema.CaseClass2[String, String, PreBilled] = DeriveSchema.gen[PreBilled]
    }
    final case class Billed(id: String, sku: String, cost: Int) extends Invoice
    object Billed    {
      implicit val schema: Schema.CaseClass3[String, String, Int, Billed] = DeriveSchema.gen[Billed]
    }
    implicit val schema: Schema[Invoice] = DeriveSchema.gen[Invoice]
  }

  // get the rendered json string from a case class
  val preBilled  = Invoice.PreBilled("id", "sku")
  val jsonString = preBilled.toJsonString[Invoice] // requires "import zio.dynamodb.json._"
  println(jsonString) // {"sku":{"S":"sku"},"id":{"S":"id"},"invoiceType":{"S":"PreBilled"}}
  println(preBilled.toJsonStringPretty[Invoice])

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

  // random AttrMap with no Schema
  val attrMap = AttrMap("foo" -> "foo", "bar" -> "bar", "count" -> 1)
  println(attrMap.toJsonString) // {"foo":{"S":"foo"},"bar":{"S":"bar"},"baz":{"S":"baz"}}
  println(attrMap.toJsonStringPretty)
}
