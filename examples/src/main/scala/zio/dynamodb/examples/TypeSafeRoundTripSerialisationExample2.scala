package zio.dynamodb.examples

import zio.Console.printLine
import zio.ZIOAppDefault
import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.examples.TypeSafeRoundTripSerialisationExample2.Invoice2.{
  Address,
  Billed2,
  LineItem,
  PaymentType,
  PreBilled2,
  Product
}
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery, PrimaryKey }
import zio.schema.annotation.{ caseName, discriminatorName }
import zio.schema.{ DeriveSchema, Schema }

import java.time.Instant
import zio.dynamodb.ProjectionExpression
import zio.ZIO
import zio.dynamodb.DynamoDBError

/*
[info] item=Some(AttrMap(HashMap(lineItems -> List(Chunk(Map(Map(String(product) -> Map(Map(String(name) -> String(a), String(sku) -> String(sku1))), String(price) -> Number(1.0), String(itemId) -> String(lineItem1))),Map(Map(String(product) -> Map(Map(String(name) -> String(b), String(sku) -> String(sku2))), String(price) -> Number(2.0), String(itemId) -> String(lineItem2))))), categoryMap -> Map(Map(String(a) -> String(1), String(b) -> String(2))), paymentType -> String(debit), total -> Number(10.0), id -> String(1), sequence -> Number(1), isTest -> Bool(false), dueDate -> String(2023-07-16T08:50:00.898031Z), address -> Map(Map(String(country) -> String(UK), String(line1) -> String(line1))), accountSet -> StringSet(Set(account1, account2)))))
 */

//import TypeSafeRoundTripSerialisationExample2.Invoice2.{ Billed2, PreBilled2 }

object TypeSafeRoundTripSerialisationExample2 extends ZIOAppDefault {

  @discriminatorName("invoiceType")
  sealed trait Invoice2 {
    def id: String
  }
  object Invoice2       {
    @enumOfCaseObjects
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

    final case class Billed2(
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
    ) extends Invoice2
    object Billed2 {
      implicit val schema: Schema.CaseClass10[String, Int, Instant, BigDecimal, Boolean, Map[String, String], Set[
        String
      ], Option[Address], List[LineItem], PaymentType, Billed2] = DeriveSchema.gen[Billed2]

      val (id, sequence, dueDate, total, isTest, categoryMap, accountSet, address, lineItems, paymentType) =
        ProjectionExpression.accessors[Billed2]
    }

    final case class PreBilled2(
      id: String,
      sequence: Int,
      dueDate: Instant,
      total: BigDecimal
    ) extends Invoice2
    object PreBilled2 {
      implicit val schema: Schema.CaseClass4[String, Int, Instant, BigDecimal, PreBilled2] =
        DeriveSchema.gen[PreBilled2]
      val (id, sequence, dueDate, total)                                                   = ProjectionExpression.accessors[PreBilled2]
    }

    implicit val schema: Schema[Invoice2] = DeriveSchema.gen[Invoice2]
  }

  private val billedInvoice: Billed2                = Billed2(
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
  private val preBilledInvoice: Invoice2.PreBilled2 = Invoice2.PreBilled2(
    id = "2",
    sequence = 2,
    dueDate = Instant.now(),
    total = BigDecimal(20.0)
  )

  import zio.dynamodb.KeyConditionExpr

  object Repository {
    def genericFindById[A <: Invoice2](
      pkExpr: KeyConditionExpr.PartitionKeyExpr[A, String]
    )(implicit ev: Schema[A]): ZIO[DynamoDBExecutor, Throwable, Either[DynamoDBError, Invoice2]] =
      DynamoDBQuery.get2("table1", pkExpr).execute

    def genericSave[A <: Invoice2](
      invoice: A
    )(implicit ev: Schema[A]): ZIO[DynamoDBExecutor, Throwable, Option[Invoice2]] =
      DynamoDBQuery.put("table1", invoice).execute
  }

  private val program = for {
    _      <- Repository.genericSave(billedInvoice)
    _      <- Repository.genericSave(preBilledInvoice)
    found  <- Repository.genericFindById(Billed2.id.primaryKey === "1")
    found2 <- Repository.genericFindById(PreBilled2.id.primaryKey === "2")
    item   <- DynamoDBQuery.getItem("table1", PrimaryKey("id" -> "1")).execute
    _      <- printLine(s"found=$found")
    _      <- printLine(s"found2=$found2")
    _      <- printLine(s"item=$item")
  } yield ()

  override def run =
    program.provide(DynamoDBExecutor.test("table1" -> "id"))
}
