package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.{ getItem, put }
import zio.Scope
import zio.test.Spec
import zio.test.assertTrue
import zio.test.TestEnvironment
import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.schema.annotation.discriminatorName
import zio.test.TestAspect
import zio.dynamodb.DynamoDBQuery.getWithNarrow

object TypeSafeApiNarrowSpec extends DynamoDBLocalSpec {

  object dynamo {
    @discriminatorName("invoiceType")
    sealed trait Invoice {
      def id: String
    }
    object Invoice       {
      final case class Unpaid(id: String) extends Invoice
      object Unpaid {
        implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]
        val id                                                 = ProjectionExpression.accessors[Unpaid]
      }
      final case class Paid(id: String, amount: Int) extends Invoice
      object Paid   {
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
    test("getWithNarrow succeeds in narrowing an Invoice to Unpaid") {
      withSingleIdKeyTable { invoiceTable =>
//        val key: ProjectionExpression[dynamo.Invoice, String]            = dynamo.Invoice.unpaid >>> dynamo.Invoice.Unpaid.id
        val keyCond = dynamo.Invoice.Unpaid.id.partitionKey === "1"
        for {
          _    <- put[dynamo.Invoice](invoiceTable, dynamo.Invoice.Unpaid("1")).execute
          item <- getItem(invoiceTable, PrimaryKey("id" -> "1")).execute

          unpaid <- getWithNarrow(dynamo.Invoice.unpaid)(invoiceTable)(keyCond).execute.absolve
        } yield {
          val unpaid2: dynamo.Invoice.Unpaid = unpaid
          val ensureDiscriminatorPresent     = item == Some(Item("id" -> "1", "invoiceType" -> "Unpaid"))
          assertTrue(unpaid2 == dynamo.Invoice.Unpaid("1") && ensureDiscriminatorPresent)
        }
      }
    }
  )

}
