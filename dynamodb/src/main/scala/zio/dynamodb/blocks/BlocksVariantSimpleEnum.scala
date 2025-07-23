package zio.dynamodb.blocks

import zio.blocks.schema._

object BlocksVariantSimpleEnum extends App {
  sealed trait Payment
  object Payment {
    case object Cash       extends Payment {
      lazy implicit val schema: Schema[Cash.type] = Schema.derived
    }
    case object CreditCard extends Payment {
      lazy implicit val schema: Schema[CreditCard.type] = Schema.derived
    }
    lazy implicit val schema: Schema[Payment] = Schema.derived
  }
  final case class Person(id: String, payment: Payment)
  object Person extends CompanionOptics[Person] {
    lazy implicit val schema: Schema[Person] = Schema.derived
    val id: Lens[Person, String]             = optic(_.id)
    val payment: Lens[Person, Payment]       = optic(_.payment)
  }
  // ================================================================================================

  val dv = Person.schema.toDynamicValue(Person("1", Payment.Cash))
  println(s"XXXXXX dv: $dv")
}
