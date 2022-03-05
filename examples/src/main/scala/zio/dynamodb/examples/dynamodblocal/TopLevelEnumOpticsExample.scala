package zio.dynamodb.examples.dynamodblocal

import zio.dynamodb.DynamoDBQuery.queryAll
import zio.dynamodb.{ DynamoDBQuery, ProjectionExpression }
import zio.dynamodb.examples.dynamodblocal.StudentZioDynamoDbExampleWithOptics.Student
import zio.dynamodb.examples.dynamodblocal.TopLevelEnumOpticsExample.Invoice.{ BilledInvoice, PreBilledInvoice }
import zio.schema.{ DeriveSchema, Schema }
import zio.stream

object TopLevelEnumOpticsExample {
  sealed trait Invoice {
    def id: String
  }
  object Invoice       {
    final case class PreBilledInvoice(
      id: String,
      sku: String
    ) extends Invoice
    object PreBilledInvoice {
      implicit val schema: Schema.CaseClass2[String, String, PreBilledInvoice] = DeriveSchema.gen[PreBilledInvoice]
    }

    final case class BilledInvoice(
      id: String,
      sku: String,
      amount: Double
    ) extends Invoice
    object BilledInvoice {
      implicit val schema: Schema.CaseClass3[String, String, Double, BilledInvoice] = DeriveSchema.gen[BilledInvoice]
    }

  }

  /*
   Explore polymorphic queries using RO for top level Enums
   */
  def polymorphicQueryByExample(invoice: Invoice): DynamoDBQuery[stream.Stream[Throwable, Student]] =
    invoice match {
      case PreBilledInvoice(id_, sku_)       =>
        val (id, sku) = ProjectionExpression.accessors[PreBilledInvoice]
        queryAll[Student]("invoice")
          .filter( // Scan/Query
            id === id_ && sku === sku_
          )
      case BilledInvoice(id_, sku_, amount_) =>
        val (id, sku, amount) = ProjectionExpression.accessors[BilledInvoice]
        queryAll[Student]("invoice")
          .filter( // Scan/Query
            id === id_ && sku === sku_ && amount === amount_
          )
    }

}
