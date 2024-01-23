package zio.dynamodb.examples

import zio.Console.printLine
import zio.ZIOAppDefault
import zio.dynamodb.examples.TypeSafeRoundTripSerialisationExample.Invoice.{
  Address,
  Billed,
  LineItem,
  PaymentType,
  PreBilled,
  Product
}
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery, PrimaryKey }
import zio.schema.annotation.{ caseName, discriminatorName }
import zio.schema.{ DeriveSchema, Schema }

import java.time.Instant
import zio.dynamodb.ProjectionExpression
import zio.ZIO
import zio.dynamodb.DynamoDBError.ItemError
object TypeSafeRoundTripSerialisationExample extends ZIOAppDefault {

  @discriminatorName("invoiceType")
  sealed trait Invoice {
    def id: String
  }
  object Invoice       {
    sealed trait PaymentType
    object PaymentType {
      @caseName("debit")
      case object DebitCard  extends PaymentType
      @caseName("credit")
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
    object Billed {
      implicit val schema: Schema.CaseClass10[String, Int, Instant, BigDecimal, Boolean, Map[String, String], Set[
        String
      ], Option[Address], List[LineItem], PaymentType, Billed] = DeriveSchema.gen[Billed]

      val (id, sequence, dueDate, total, isTest, categoryMap, accountSet, address, lineItems, paymentType) =
        ProjectionExpression.accessors[Billed]
    }

    final case class PreBilled(
      id: String,
      sequence: Int,
      dueDate: Instant,
      total: BigDecimal
    ) extends Invoice
    object PreBilled {
      implicit val schema: Schema.CaseClass4[String, Int, Instant, BigDecimal, PreBilled] =
        DeriveSchema.gen[PreBilled]
      val (id, sequence, dueDate, total)                                                  = ProjectionExpression.accessors[PreBilled]
    }

    implicit val schema: Schema[Invoice] = DeriveSchema.gen[Invoice]
  }

  private val billedInvoice: Billed               = Billed(
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
  private val preBilledInvoice: Invoice.PreBilled = Invoice.PreBilled(
    id = "2",
    sequence = 2,
    dueDate = Instant.now(),
    total = BigDecimal(20.0)
  )

  import zio.dynamodb.KeyConditionExpr

  object Repository {
    def genericFindById[A <: Invoice](
      pkExpr: KeyConditionExpr.PartitionKeyEquals[A]
    )(implicit ev: Schema[A]): ZIO[DynamoDBExecutor, Throwable, Either[ItemError, Invoice]] =
      DynamoDBQuery.get("table1")(pkExpr).execute

    def genericSave[A <: Invoice](
      invoice: A
    )(implicit ev: Schema[A]): ZIO[DynamoDBExecutor, Throwable, Option[Invoice]] =
      DynamoDBQuery.put("table1", invoice).execute
  }

  private val program = for {
    _      <- Repository.genericSave(billedInvoice)
    _      <- Repository.genericSave(preBilledInvoice)
    found  <- Repository.genericFindById(Billed.id.partitionKey === "1")
    found2 <- Repository.genericFindById(PreBilled.id.partitionKey === "2")
    item   <- DynamoDBQuery.getItem("table1", PrimaryKey("id" -> "1")).execute
    _      <- printLine(s"found=$found")
    _      <- printLine(s"found2=$found2")
    _      <- printLine(s"item=$item")
  } yield ()

  override def run =
    program.provide(DynamoDBExecutor.test("table1" -> "id"))
}
