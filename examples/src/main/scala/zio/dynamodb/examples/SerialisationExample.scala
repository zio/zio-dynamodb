package zio.dynamodb.examples

import zio.dynamodb.{ AttrMap, Item }

import java.time.{ Instant, ZoneOffset }

object SerialisationExample extends App {

  final case class LineItem(itemId: String, price: BigDecimal, product: Product)
  final case class Product(sku: String, name: String)
  final case class Address(line1: String, line2: Option[String], country: String)
  final case class Invoice(
    id: String,
    sequence: Int,
    dueDate: Instant,
    total: BigDecimal,
    isTest: Boolean,
    categoryMap: Map[String, String],
    categorySet: Set[String],
    optSet: Option[Set[String]],
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
    optSet = Some(Set("a", "b")),
    address = Some(Address("line1", /*Some("line2")*/ None, "UK")),
    lineItems = List(
      LineItem("lineItem1", BigDecimal(1.0), Product("sku1", "a")),
      LineItem("lineItem2", BigDecimal(2.0), Product("sku2", "b"))
    )
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
      "optSet"      -> i.optSet.orNull, // Scala Null is used to encode None for Option fields
      "address"     -> i.address.map { addr =>
        Item(
          "line1"   -> addr.line1,
          "line2"   -> addr.line2, // addr.line2.orNull results in Some(null) as Option based type class is invoked rather than Null one
          "country" -> addr.country
        )
      }.orNull,
      "lineItems"   -> i.lineItems.map(li =>
        Item(
          "itemId"  -> li.itemId,
          "price"   -> li.price,
          "product" -> Item(
            "sku"  -> li.product.sku,
            "name" -> li.product.name
          )
        )
      )
    )

  println("invoiceToAttrMap: " + invoiceToAttrMap(invoice1))

  def attrMapToInvoice(m: AttrMap): Either[String, Invoice] =
    for {
      id          <- m.get[String]("id")
      sequence    <- m.get[Int]("sequence")
      // TODO: getItem example
      dueDate     <- m.get[String]("dueDate")
      total       <- m.get[BigDecimal]("total")
      isTest      <- m.get[Boolean]("isTest")
      categoryMap <- m.get[Map[String, String]]("categoryMap")
      categorySet <- m.get[Set[String]]("categorySet")
      optSet      <- m.getOptional[Set[String]]("optSet")
      address     <- m.getOptionalItem("address") { m2 =>
                       m2.as(Address)("line1", "line2", "country")
                     }
      lineItems   <- m.getIterableItem[LineItem]("lineItems") { m2 =>
                       for {
                         itemId  <- m2.get[String]("itemId")
                         price   <- m2.get[BigDecimal]("price")
                         product <- m2.getItem[Product]("product") { m =>
                                      m.as(Product)("sku", "name")
                                    }
                       } yield LineItem(itemId, price, product)
                     }
    } yield Invoice(
      id,
      sequence,
      stringToDate(dueDate),
      total,
      isTest,
      categoryMap,
      categorySet,
      optSet,
      address,
      lineItems.toSeq
    )

  // I think problem is Null is bottom type of most things
  def foo[A](a: Option[A]): Option[Any] =
    a match {
      case Some(a) => Some(a)
      case _       => null
    }

  println("attrMapToInvoice: " + attrMapToInvoice(invoiceToAttrMap(invoice1)))

}
