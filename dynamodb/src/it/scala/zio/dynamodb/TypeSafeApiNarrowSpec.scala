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
import zio.dynamodb.TypeSafeApiNarrowSpec.dynamo.Invoice.Paid.Paid1
import zio.dynamodb.TypeSafeApiNarrowSpec.dynamo.Invoice.Paid.Paid2

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

      sealed trait Paid extends Invoice {
        def amount: Int
      }

      object Paid {
        final case class Paid1(id: String, amount: Int) extends Paid

        object Paid1 {
          implicit val schema: Schema.CaseClass2[String, Int, Paid1] = DeriveSchema.gen[Paid1]
          val (id, amount)                                           = ProjectionExpression.accessors[Paid1]
        }

        final case class Paid2(id: String, amount: Int, sku: String) extends Paid

        object Paid2 {
          implicit val schema: Schema.CaseClass3[String, Int, String, Paid2] = DeriveSchema.gen[Paid2]
          val (id, amount, sku)                                              = ProjectionExpression.accessors[Paid2]
        }

        implicit val schema: Schema.Enum2[Paid1, Paid2, Paid] = DeriveSchema.gen[Paid]
        val (paid, paid2)                                     = ProjectionExpression.accessors[Paid]
      }

      implicit val schema: Schema.Enum3[Unpaid, Paid1, Paid2, Invoice] = DeriveSchema.gen[Invoice]
      val (unpaid, paid, paid2)                                        = ProjectionExpression.accessors[Invoice]
    }

  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("TypeSafeApiNarrowSpec")(
      topLevelSumTypeNarrowSuite,
      narrowSuite
    ) @@ TestAspect.nondeterministic

  val topLevelSumTypeNarrowSuite = suite("for top level Invoice sum type with @discriminatorName annotation")(
    test("put with narrow") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _    <- DynamoDBQuery
                    .putWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable, dynamo.Invoice.Unpaid("1"))
                    .where(
                      !dynamo.Invoice.Unpaid.id.exists
                    ) // note expressions are of concrete type Unpaid eg ConditionExpression[dynamo.Invoice.Unpaid]
                    .execute
          item <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute

          unpaid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Unpaid = unpaid
          val ensureDiscriminatorPresent     = item == Some(Item("id" -> "1", "invoiceType" -> "Unpaid"))
          assertTrue(unpaid2 == dynamo.Invoice.Unpaid("1") && ensureDiscriminatorPresent)
        }
      }
    },
    test("getWithNarrow succeeds in narrowing an Unpaid Invoice instance to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _ <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Unpaid("1")).execute

          unpaid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Unpaid = unpaid
          assertTrue(unpaid2 == dynamo.Invoice.Unpaid("1"))
        }
      }
    },
    test("getWithNarrow succeeds in narrowing an Paid1 Invoice instance to Paid1") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Paid.Paid1] =
          dynamo.Invoice.Paid.Paid1.id.partitionKey === "1"
        for {
          _ <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Paid.Paid1("1", 42)).execute

          paid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid.Paid1](invoiceTable)(keyCond).execute.absolve
        } yield {
          val paid2: dynamo.Invoice.Paid.Paid1 = paid
          assertTrue(paid2 == dynamo.Invoice.Paid.Paid1("1", 42))
        }
      }
    },
    test("getWithNarrow succeeds in narrowing an Paid1 Invoice instance to Paid") {
      withSingleIdKeyTable { invoiceTable =>
        val pe1: ProjectionExpression[dynamo.Invoice.Paid, String]        =
          dynamo.Invoice.paid >>> dynamo.Invoice.Paid.Paid1.id
        val pk1: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Paid] = pe1.partitionKey === "1"
        println(s"pk1: $pk1")

        val x: ProjectionExpression[dynamo.Invoice.Paid, String]          =
          ProjectionExpression.foo[dynamo.Invoice.Paid, String]("id")
        val pk2: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Paid] = x.partitionKey === "1"
        println(s"pk1: $pk2")

        for {
          _ <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Paid.Paid1("1", 42)).execute

          paid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid](invoiceTable)(pk2).execute.absolve
        } yield {
          val paid2: dynamo.Invoice.Paid = paid
          assertTrue(paid2 == dynamo.Invoice.Paid.Paid1("1", 42))
        }
      }
    },
    test("getWithNarrow fails in narrowing an Unpaid Invoice instance to Paid1") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Paid.Paid1] =
          dynamo.Invoice.Paid.Paid1.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Unpaid("1")).execute
          exit <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Paid.Paid1](invoiceTable)(keyCond).execute.absolve.exit
        } yield assert(exit)(
          fails(equalTo(ItemError.DecodingError("failed to narrow - found type Unpaid but expected type Paid1")))
        )
      }
    },
    test("getWithNarrow fails in narrowing a Paid1 Invoice instance to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Paid.Paid1("1", 42)).execute
          exit <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve.exit
        } yield assert(exit)(
          fails(equalTo(ItemError.DecodingError("failed to narrow - found type Paid1 but expected type Unpaid")))
        )
      }
    }
  )

  val narrowSuite = suite("narrow suite")(
    test("narrow Paid instance to Paid for success and failure") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Paid.Paid1("1", 1)
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid.Paid1](invoice)
      val invalid                 = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoice)

      assert(valid)(isRight) && assert(invalid)(
        isLeft(equalTo("failed to narrow - found type Paid1 but expected type Unpaid"))
      )
    },
    test("narrow Unpaid instance to Unpaid for success and failure") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Unpaid("1")
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoice)
      val invalid                 = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid.Paid1](invoice)

      assert(valid)(isRight) && assert(invalid)(
        isLeft(equalTo("failed to narrow - found type Unpaid but expected type Paid1"))
      )
    },
    test("narrow Paid1 instance to Paid") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Paid.Paid1("1", 42)
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid](invoice)

      assert(valid)(isRight)
    }
  )
}
