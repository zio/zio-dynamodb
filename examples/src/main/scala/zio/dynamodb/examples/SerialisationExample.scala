package zio.dynamodb.examples

import zio.dynamodb.{ AttrMap, Item }

import java.time.{ Instant, ZoneOffset }

object SerialisationExample extends App {
  final case class LineItem(itemId: String, price: BigDecimal)
  final case class Address(line1: String, line2: Option[String], country: String)
  final case class Invoice(
    id: String,
    sequence: Int,
    dueDate: Instant,
    total: BigDecimal,
    isTest: Boolean,
    categoryMap: Map[String, String],
    categorySet: Set[String],
    address: Option[Address],
    lineItems: Seq[LineItem]
  )

  val invoice1 = Invoice(
    id = "1",
    sequence = 1,
    dueDate = Instant.now(),
    total = BigDecimal(10.0),
    isTest = false,
    categoryMap = Map("a" -> "1", "b" -> "2"),
    categorySet = Set("a", "b"),
    address = Some(Address("line1", Some("line2"), "UK")),
    lineItems = List(LineItem("lineItem1", BigDecimal(1.0)), LineItem("lineItem2", BigDecimal(2.0)))
  )

  def dateToString(d: Instant): String = d.atOffset(ZoneOffset.UTC).toString
  def stringToDate(d: String): Instant = Instant.parse(d)

  def invoiceToAttrMap(i: Invoice): Item =
    Item(
      "id"          -> i.id,
      "sequence"    -> i.sequence,
      "dueDate"     -> dateToString(i.dueDate),
      "total"       -> i.total,
      "isTest"      -> i.isTest,
      "categoryMap" -> i.categoryMap,
      "categorySet" -> i.categorySet,
      "address"     -> i.address.map { addr =>
        Item(
          "line1"   -> addr.line1,
          "line2"   -> addr.line2.orNull,
          "country" -> addr.country
        )
      }.orNull,
      "lineItems"   -> i.lineItems.map(li =>
        Item(
          "itemId" -> li.itemId,
          "price"  -> li.price
        )
      )
    )

  println("invoiceToAttrMap: " + invoiceToAttrMap(invoice1))

  def attrMapToInvoice(m: AttrMap): Either[String, Invoice] =
    for {
      id          <- m.get[String]("id")
      sequence    <- m.get[Int]("sequence")
      dueDate     <- m.get[String]("dueDate")
      total       <- m.get[BigDecimal]("total")
      isTest      <- m.get[Boolean]("isTest")
      categoryMap <- m.get[Map[String, String]]("categoryMap")
      categorySet <- m.get[Set[String]]("categorySet")
      address     <- m.getOptionalItem("address") { m =>
                       for {
                         line1   <- m.get[String]("line1")
                         line2   <- m.getOpt[String]("line2")
                         country <- m.get[String]("country")
                       } yield Some(Address(line1, line2, country))
                     }

      lineItems   <- m.getIterableItem[LineItem]("lineItems") { m =>
                       for {
                         itemId <- m.get[String]("itemId")
                         price  <- m.get[BigDecimal]("price")
                       } yield LineItem(itemId, price)
                     }
    } yield Invoice(
      id,
      sequence,
      stringToDate(dueDate),
      total,
      isTest,
      categoryMap,
      categorySet,
      address,
      lineItems.toSeq
    )

  println("attrMapToInvoice: " + attrMapToInvoice(invoiceToAttrMap(invoice1)))

}
