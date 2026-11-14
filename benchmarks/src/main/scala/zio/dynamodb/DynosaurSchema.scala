/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

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
        field.opt("address", _.address)
//        field("map", _.map),
//        field("list", _.list),
//        field("tuple", _.tuple),
//        field("light", _.light)
//        field("paymentType", _.paymentMethod)
      ).mapN(Person.apply)
    }

  // BigRecord, written by hand: Dynosaur has no derivation. Its 26 fields exceed mapN's 22, so the record is
  // built from two 13-field groups.
  implicit lazy val prioritySchema: Schema[Priority] = Schema.string.imap[Priority] {
    case "Low"      => Priority.Low
    case "Medium"   => Priority.Medium
    case "High"     => Priority.High
    case "Critical" => Priority.Critical
    case other      => throw new Exception(s"Unknown Priority: $other")
  }(_.toString)

  implicit lazy val fulfilmentSchema: Schema[Fulfilment] = Schema.oneOf[Fulfilment] { alt =>
    val warehouse: Schema[Fulfilment.Warehouse] =
      Schema
        .record[Fulfilment.Warehouse](field => field("code", _.code).map(Fulfilment.Warehouse.apply))
        .tag("Warehouse")
    val dropShip: Schema[Fulfilment.DropShip]   = Schema
      .record[Fulfilment.DropShip] { field =>
        (field("vendor", _.vendor), field("leadDays", _.leadDays)).mapN(Fulfilment.DropShip.apply)
      }
      .tag("DropShip")
    val pickup: Schema[Fulfilment.Pickup.type]  =
      Schema.string.imap[Fulfilment.Pickup.type](_ => Fulfilment.Pickup)(_ => "Pickup").tag("Pickup")
    alt(warehouse) |+| alt(dropShip) |+| alt(pickup)
  }

  implicit lazy val contactSchema: Schema[Contact] = Schema.oneOf[Contact] { alt =>
    val email: Schema[Contact.Email] =
      Schema.record[Contact.Email](field => field("address", _.address).map(Contact.Email.apply)).tag("Email")
    val phone: Schema[Contact.Phone] = Schema
      .record[Contact.Phone] { field =>
        (field("number", _.number), field.opt("ext", _.ext)).mapN(Contact.Phone.apply)
      }
      .tag("Phone")
    alt(email) |+| alt(phone)
  }

  implicit lazy val addressSchema: Schema[Address] = Schema.record { field =>
    (
      field("street", _.street),
      field("city", _.city),
      field("state", _.state),
      field("zip", _.zip),
      field("country", _.country)
    ).mapN(Address.apply)
  }

  implicit lazy val geoPointSchema: Schema[GeoPoint] = Schema.record { field =>
    (field("lat", _.lat), field("lng", _.lng)).mapN(GeoPoint.apply)
  }

  implicit lazy val auditInfoSchema: Schema[AuditInfo] = Schema.record { field =>
    (
      field("createdBy", _.createdBy),
      field("createdAt", _.createdAt),
      field("updatedBy", _.updatedBy),
      field("updatedAt", _.updatedAt),
      field("version", _.version)
    ).mapN(AuditInfo.apply)
  }

  implicit lazy val moneySchema: Schema[Money] = Schema.record { field =>
    (field("currency", _.currency), field("amountMinor", _.amountMinor)).mapN(Money.apply)
  }

  implicit lazy val dimensionsSchema: Schema[Dimensions] = Schema.record { field =>
    (
      field("widthMm", _.widthMm),
      field("heightMm", _.heightMm),
      field("depthMm", _.depthMm),
      field("weightG", _.weightG)
    ).mapN(Dimensions.apply)
  }

  implicit lazy val bigRecordSchema: Schema[BigRecord] = Schema.record { field =>
    val first  = (
      field("id", _.id),
      field("sku", _.sku),
      field("name", _.name),
      field("description", _.description),
      field("category", _.category),
      field("subCategory", _.subCategory),
      field("brand", _.brand),
      field("priority", _.priority),
      field("fulfilment", _.fulfilment),
      field("primaryContact", _.primaryContact),
      field.opt("secondaryContact", _.secondaryContact),
      field("billingAddress", _.billingAddress),
      field.opt("shippingAddress", _.shippingAddress)
    ).tupled
    val second = (
      field("location", _.location),
      field("audit", _.audit),
      field("price", _.price),
      field("cost", _.cost),
      field("dimensions", _.dimensions),
      field("tags", _.tags),
      field("ratings", _.ratings),
      field("attributes", _.attributes),
      field("quantityOnHand", _.quantityOnHand),
      field("reorderPoint", _.reorderPoint),
      field("discontinued", _.discontinued),
      field("averageRating", _.averageRating),
      field.opt("notes", _.notes)
    ).tupled
    (first, second).mapN {
      case (
            (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13),
            (b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13)
          ) =>
        BigRecord(
          a1,
          a2,
          a3,
          a4,
          a5,
          a6,
          a7,
          a8,
          a9,
          a10,
          a11,
          a12,
          a13,
          b1,
          b2,
          b3,
          b4,
          b5,
          b6,
          b7,
          b8,
          b9,
          b10,
          b11,
          b12,
          b13
        )
    }
  }
}
