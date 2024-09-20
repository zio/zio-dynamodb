package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ getItem, put }
import zio.Scope
import zio.test.Spec
import zio.test.Assertion.{ containsString, fails, hasMessage }
import zio.test.{ assert, assertTrue }
import zio.test.TestEnvironment
import zio.test.Assertion.{ isLeft, isRight }
import zio.test.TestAspect
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName
import zio.dynamodb.DynamoDBQuery.getWithNarrow

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
      object Paid      {
        implicit val schema: Schema.CaseClass2[String, Int, Paid] = DeriveSchema.gen[Paid]
        val (id, amount)                                          = ProjectionExpression.accessors[Paid]
      }
      implicit val schema: Schema.Enum2[Unpaid, Paid, Invoice] =
        DeriveSchema.gen[Invoice]
      val (unpaid, paid) = ProjectionExpression.accessors[Invoice]
    }

  }

  override def spec: Spec[Environment with TestEnvironment with Scope, Any] =
    suite("TypeSafeApiMappingSpec")(
      topLevelSumTypeDiscriminatorNameSuite
    ) @@ TestAspect.nondeterministic

  val topLevelSumTypeDiscriminatorNameSuite = suite("with @discriminatorName annotation")(
    test("getWithNarrow succeeds in narrowing an Unpaid Invoice instance to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
        val keyCond: KeyConditionExpr.PartitionKeyEquals[dynamo.Invoice.Unpaid] =
          dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Unpaid("1")).execute
          item <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute

          unpaid <- getWithNarrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoiceTable)(keyCond).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Unpaid = unpaid
          val ensureDiscriminatorPresent     = item == Some(Item("id" -> "1", "invoiceType" -> "Unpaid"))
          assertTrue(unpaid2 == dynamo.Invoice.Unpaid("1") && ensureDiscriminatorPresent)
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
        } yield assert(exit)(fails(hasMessage(containsString("failed to narrow"))))
      }
    },
    test("narrow") {
      val invoice: dynamo.Invoice = dynamo.Invoice.Paid("1", 1)
      val valid                   = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Paid](invoice)
      val invalid                 = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unpaid](invoice)

      // gives expected compile error:
      // type arguments [zio.dynamodb.TypeSafeApiNarrowSpec.dynamo.Invoice,zio.dynamodb.TypeSafeApiNarrowSpec.dynamo.Invoice.Unrelated] do not conform to method narrow's type parameter bounds [From,To <: From]
      //      val unrelated                       = DynamoDBQuery.narrow[dynamo.Invoice, dynamo.Invoice.Unrelated](Unrelated(1))

      // gives expected compile error:
      // could not find implicit value for evidence parameter of type zio.schema.Schema.Enum[zio.dynamodb.TypeSafeApiNarrowSpec.dynamo.Invoice.Unrelated]
      //      val x                       = DynamoDBQuery.narrow[dynamo.Invoice.Unrelated, dynamo.Invoice.Unrelated](Unrelated(1))

      assert(valid)(isRight) && assert(invalid)(isLeft)
    }
  )

}
