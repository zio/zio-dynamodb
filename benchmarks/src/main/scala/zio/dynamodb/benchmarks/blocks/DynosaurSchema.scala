package zio.dynamodb.benchmarks.blocks

import cats.implicits._
import dynosaur.Schema
object DynosaurSchema {
  import ListOfRecordsDomain._

  implicit val paymentMethodSchema: Schema[PaymentMethod] =
    Schema.string.imap[PaymentMethod] {
      case "DebitCard"                      => PaymentMethod.DebitCard
      case "Paypal"                         => PaymentMethod.Paypal
      case s if s.startsWith("CreditCard:") =>
        val parts = s.stripPrefix("CreditCard:").split(":")
        PaymentMethod.CreditCard(parts(0), parts(1).toInt)
    } {
      case PaymentMethod.DebitCard          => "DebitCard"
      case PaymentMethod.Paypal             => "Paypal"
      case PaymentMethod.CreditCard(n, cvv) => s"CreditCard:$n:$cvv"
    }

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
        field("address", _.address),
        field("childrenAges", _.childrenAges)
//        field("paymentMethod", _.paymentMethod)
      ).mapN(Person.apply)
    }

}
