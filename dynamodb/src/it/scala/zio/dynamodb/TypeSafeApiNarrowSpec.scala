package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ getItem, put }
import zio.Scope
import zio.test.Spec
import zio.test.Assertion.fails
import zio.test.{ assert, assertTrue }
import zio.test.TestEnvironment
import zio.test.Assertion.{ equalTo, isLeft, isRight }
import zio.test.TestAspect
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName
import zio.dynamodb.DynamoDBQuery.getWithNarrow
import zio.dynamodb.DynamoDBError.ItemError

object TypeSafeApiNarrowSpec extends DynamoDBLocalSpec {

  object dynamo {
    @discriminatorName("invoiceType")
    sealed trait Invoice {
      def id: String
    }
    object Invoice       {
      final case class Unrelated(id: Int)
      object Unrelated {
        implicit val schema: Schema.CaseClass1[Int, Unrelated] = DeriveSchema.gen[Unrelated]
        val id                                                 = ProjectionExpression.accessors[Unrelated]
      }
      final case class Unpaid(id: String) extends Invoice
      object Unpaid    {
        implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]
        val id                                                 = ProjectionExpression.accessors[Unpaid]
      }

      final case class Paid(id: String, amount: Int) extends Invoice
      object Paid {
        implicit val schema: Schema.CaseClass2[String, Int, Paid] = DeriveSchema.gen[Paid]
        val (id, amount)                                          = ProjectionExpression.accessors[Paid]
      }

      implicit val schema: Schema.Enum2[Unpaid, Paid, Invoice] = DeriveSchema.gen[Invoice]
      val (unpaid, paidA)                                      = ProjectionExpression.accessors[Invoice]
    }

  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("TypeSafeApiNarrowSpec")(
      topLevelSumTypeNarrowSuite,
      narrowSuite
    ) @@ TestAspect.nondeterministic

  val topLevelSumTypeNarrowSuite = suite("for top level Invoice sum type with @discriminatorName annotation")(
    test("put with narrow to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _         <- DynamoDBQuery
                         .putWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable, dynamo.Invoice.Unpaid("1"))
                         .where(
                           !dynamo.Invoice.Unpaid.id.exists
                         ) // note expressions are of concrete type Unpaid eg ConditionExpression[dynamo.Invoice.Unpaid]
                         .execute
          maybeItem <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute

          unpaid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Unpaid = unpaid
          assertTrue(unpaid2 == dynamo.Invoice.Unpaid("1") && hasDiscriminator(maybeItem)("Unpaid"))
        }
      }
    },
    test("put with narrow to Paid") {
      withSingleIdKeyTable { invoiceTable =>
        for {
          _         <- DynamoDBQuery
                         .putWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid](invoiceTable, dynamo.Invoice.Paid("1", 42))
                         .where(!dynamo.Invoice.Paid.id.exists)
                         .execute
          maybeItem <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute
          paid      <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid](invoiceTable)(
                         dynamo.Invoice.Paid.id.partitionKey === "1"
                       ).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Paid = paid
          assertTrue(unpaid2 == dynamo.Invoice.Paid("1", 42) && hasDiscriminator(maybeItem)("Paid"))
        }
      }
    },
    test("getWithNarrow fails in narrowing an Unpaid Invoice instance to Paid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Paid] =
          dynamo.Invoice.Paid.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Unpaid("1")).execute
          exit <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid](invoiceTable)(keyCond).execute.absolve.exit
        } yield assert(exit)(
          fails(equalTo(ItemError.DecodingError("failed to narrow - found type Unpaid but expected type Paid")))
        )
      }
    },
    test("getWithNarrow fails in narrowing a Paid Invoice instance to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Paid("1", 42)).execute
          exit <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve.exit
        } yield assert(exit)(
          fails(equalTo(ItemError.DecodingError("failed to narrow - found type Paid but expected type Unpaid")))
        )
      }
    }
  )

  val narrowSuite = suite("narrow suite")(
    test("narrow Paid instance to Paid for success and failure") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Paid("1", 1)
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid](invoice)
      val invalid                 = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoice)

      assert(valid)(isRight) && assert(invalid)(
        isLeft(equalTo("failed to narrow - found type Paid but expected type Unpaid"))
      )
    },
    test("narrow Unpaid instance to Unpaid for success and failure") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Unpaid("1")
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoice)
      val invalid                 = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid](invoice)

      assert(valid)(isRight) && assert(invalid)(
        isLeft(equalTo("failed to narrow - found type Unpaid but expected type Paid"))
      )
    }
  )

  private def hasDiscriminator(maybeItem: Option[Item])(discriminatorValue: String): Boolean =
    maybeItem match {
      case Some(item) => item.get[String]("invoiceType").contains(discriminatorValue)
      case None       => false
    }
}
