package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery.queryAll
import zio.dynamodb.examples.dynamodblocal.TopLevelSumTypeOpticsExample.Invoice.{ BilledInvoice, PreBilledInvoice }
import zio.dynamodb.{ DynamoDBQuery, ProjectionExpression }
import zio.schema.DeriveSchema
import zio.stream
import zio.schema.Schema

object TopLevelSumTypeOpticsExample extends App {
  sealed trait Invoice {
    def id: String
  }
  object Invoice       {
    final case class PreBilledInvoice(
      id: String,
      sku: String
    ) extends Invoice
    object PreBilledInvoice {
      implicit val schema: Schema.CaseClass2.WithFields["id", "sku", String, String, PreBilledInvoice] =
        DeriveSchema.gen[PreBilledInvoice]
      val (id, sku)                                                                                    = ProjectionExpression.accessors[PreBilledInvoice]
    }

    final case class BilledInvoice(
      id: String,
      sku: String,
      amount: Double
    ) extends Invoice
    object BilledInvoice {
      implicit val schema: Schema.CaseClass3.WithFields["id", "sku", "amount", String, String, Double, BilledInvoice] =
        DeriveSchema.gen[BilledInvoice]
      val (id, sku, amount)                                                                                           = ProjectionExpression.accessors[BilledInvoice]
    }

    final case class Box(invoice: Invoice)
    object Box {
      implicit val schema: Schema.CaseClass1.WithFields["invoice", Invoice, Box] = DeriveSchema.gen[Box]
      val invoice                                                                = ProjectionExpression.accessors[Box]
    }
  }

  def polymorphicQueryByExample(
    invoice: Invoice
  ): DynamoDBQuery[PreBilledInvoice with BilledInvoice, stream.Stream[Throwable, Invoice]] =
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

  val x = polymorphicQueryByExample(BilledInvoice("1", "sku1", 0.1))
  println(x)
}
