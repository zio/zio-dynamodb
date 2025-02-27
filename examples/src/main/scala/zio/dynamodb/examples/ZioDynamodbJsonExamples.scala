package zio.dynamodb.examples

import zio.dynamodb.json._
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName

import zio.schema.Schema
import zio.dynamodb.ProjectionExpression

/**
 * zio-dynamodb-json is a new experimental optional module designed for debugging and troubleshooting purposes - it should not be used in production code.
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

    final case class Person(id: Int, firstName: String, address: Either[String, List[String]])
    object Person {
      implicit val schema: Schema.CaseClass3[Int, String, Either[String, List[String]], Person] =
        DeriveSchema.gen[Person]
      final val (id, firstName, address)                                                        = ProjectionExpression.accessors[Person]
    }
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

  // decode the json string to a case class
  val errorOrClass = parse[Invoice](jsonString)
  println(errorOrClass) // Right(PreBilled("id", "sku"))

  val person1 = Invoice.Person(1, "John", Right(List("123 Main St", "456 Elm St")))
  println(person1.toJsonStringPretty[Invoice.Person])

}
