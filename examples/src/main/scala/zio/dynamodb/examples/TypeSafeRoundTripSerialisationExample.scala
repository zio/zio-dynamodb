package zio.dynamodb.examples

import zio.console.putStrLn
import zio.dynamodb.Annotations.{ constantValue, discriminator, enumOfCaseObjects }
import zio.dynamodb.examples.TypeSafeRoundTripSerialisationExample.Invoice.{
  Address,
  Billed,
  LineItem,
  PaymentType,
  Product
}
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery, PrimaryKey }
import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema, Schema }
import zio.{ App, ExitCode, URIO }

import java.time.Instant

object TypeSafeRoundTripSerialisationExample extends App {

  @discriminator("invoiceType")
  sealed trait Invoice {
    def id: String
  }
  object Invoice extends DefaultJavaTimeSchemas {
    @enumOfCaseObjects
    sealed trait PaymentType
    object PaymentType {
      @constantValue("debit")
      case object DebitCard  extends PaymentType
      @constantValue("credit")
      case object CreditCard extends PaymentType
    }

    final case class Address(line1: String, line2: Option[String], country: String)
    final case class Product(sku: String, name: String)
    final case class LineItem(itemId: String, price: BigDecimal, product: Product)

    final case class Billed(
      id: String,
      sequence: Int,
      dueDate: Instant,
      total: BigDecimal,
      isTest: Boolean,
      categoryMap: Map[String, String],
      accountSet: Set[String],
      address: Option[Address],
      lineItems: List[LineItem],
      paymentType: PaymentType
    ) extends Invoice
    final case class PreBilled(
      id: String,
      sequence: Int,
      dueDate: Instant,
      total: BigDecimal
    ) extends Invoice

    implicit val schema: Schema[Invoice] = DeriveSchema.gen[Invoice]
  }

  private val invoice1 = Billed(
    id = "1",
    sequence = 1,
    dueDate = Instant.now(),
    total = BigDecimal(10.0),
    isTest = false,
    categoryMap = Map("a" -> "1", "b" -> "2"),
    accountSet = Set("account1", "account2"),
    address = Some(Address("line1", None, "UK")),
    lineItems = List(
      LineItem("lineItem1", BigDecimal(1.0), Product("sku1", "a")),
      LineItem("lineItem2", BigDecimal(2.0), Product("sku2", "b"))
    ),
    PaymentType.DebitCard
  )

  private val program = for {
    _     <- DynamoDBQuery.put[Invoice]("table1", invoice1).execute
    found <- DynamoDBQuery.get[Invoice]("table1", PrimaryKey("id" -> "1")).execute
    item  <- DynamoDBQuery.getItem("table1", PrimaryKey("id" -> "1")).execute
    _     <- putStrLn(s"found=$found")
    _     <- putStrLn(s"item=$item")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideCustomLayer(DynamoDBExecutor.test("table1" -> "id")).exitCode
}
