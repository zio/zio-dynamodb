package zio.dynamodb.benchmarks.codecs

import cats.implicits._
import dynosaur.Schema
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object DynosaurSchema {
  import BenchmarkDomain._

  implicit val paymentMethod: Schema[PaymentMethod] = Schema.oneOf[PaymentMethod] { alt =>
    val creditCard: Schema[PaymentMethod.CreditCard] = Schema
      .record[PaymentMethod.CreditCard] { field =>
        (
          field("number", _.number),
          field("cvv", _.cvv)
        ).mapN(PaymentMethod.CreditCard.apply)
      }
      .tag("CreditCard")
    val debitCard: Schema[PaymentMethod.DebitCard]   = Schema
      .record[PaymentMethod.DebitCard] { field =>
        (
          field("number", _.number),
          field("cvv", _.cvv)
        ).mapN(PaymentMethod.DebitCard.apply)
      }
      .tag("DebitCard")
    val payPal: Schema[PaymentMethod.PayPal]         = Schema
      .record[PaymentMethod.PayPal] { field =>
        field("email", _.email).map(PaymentMethod.PayPal.apply)
      }
      .tag("PayPal")
    alt(creditCard) |+| alt(debitCard) |+| alt(payPal)
  }

  implicit val schema: Schema[TrafficLight] = Schema.attributeValue.imap[TrafficLight] { av =>
    av.s() match {
      case "Red"    => TrafficLight.Red
      case "Yellow" => TrafficLight.Yellow
      case "Green"  => TrafficLight.Green
      case other    => throw new Exception(s"Unknown TrafficLight: $other")
    }
  } { trafficLight =>
    // Encode TrafficLight to DynamoDB String
    val name = trafficLight match {
      case TrafficLight.Red    => "Red"
      case TrafficLight.Yellow => "Yellow"
      case TrafficLight.Green  => "Green"
    }
    AttributeValue.builder().s(name).build()
  }

  implicit val mapSchema: Schema[Map[String, Int]] = Schema.dict

  implicit val tupleSchema: Schema[(Int, Long, String)] = Schema.record[(Int, Long, String)] { field =>
    (
      field("0", _._1)(Schema.int),
      field("1", _._2)(Schema.long),
      field("2", _._3)(Schema.string)
    ).mapN((a, b, c) => (a, b, c))
  }

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
//        field.opt("address", _.address),
//        field("map", _.map),
//        field("list", _.list),
//        field("tuple", _.tuple),
//        field("light", _.light)
        field("paymentType", _.paymentMethod)
      ).mapN(Person.apply)
    }

}
