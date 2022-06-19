package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery.queryAll
import zio.dynamodb.examples.dynamodblocal.TopLevelSumTypeOpticsExample.Invoice.{ BilledInvoice, PreBilledInvoice }
import zio.dynamodb.{ DynamoDBQuery, ProjectionExpression }
import zio.schema.DeriveSchema
import zio.stream

object TopLevelSumTypeOpticsExample {
  sealed trait Invoice {
    def id: String
  }
  object Invoice       {
    final case class PreBilledInvoice(
      id: String,
      sku: String
    ) extends Invoice
    object PreBilledInvoice {
      implicit val schema = DeriveSchema.gen[PreBilledInvoice]
      val (id, sku)       = ProjectionExpression.accessors[PreBilledInvoice]
    }

    final case class BilledInvoice(
      id: String,
      sku: String,
      amount: Double
    ) extends Invoice
    object BilledInvoice {
      implicit val schema   = DeriveSchema.gen[BilledInvoice]
      val (id, sku, amount) = ProjectionExpression.accessors[BilledInvoice]
    }

  }

  def polymorphicQueryByExample(invoice: Invoice): DynamoDBQuery[stream.Stream[Throwable, Invoice]] =
    invoice match {
      case PreBilledInvoice(id_, sku_)       =>
        import PreBilledInvoice._
        queryAll[PreBilledInvoice]("invoice")
          .filter( // Scan/Query
            id > id_ && sku < sku_
          )
      case BilledInvoice(id_, sku_, amount_) =>
        import BilledInvoice._
        queryAll[BilledInvoice]("invoice")
          .filter( // Scan/Query
            id >= id_ && sku === sku_ && amount <= amount_
          )
    }

}
