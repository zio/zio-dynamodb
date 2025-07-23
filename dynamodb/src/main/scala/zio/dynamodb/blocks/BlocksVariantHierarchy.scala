package zio.dynamodb.blocks

import zio.blocks.schema._

object BlocksVariantHierarchy extends App {
  sealed trait Agreement {
    def id: String
  }
  object Agreement                                                          extends CompanionOptics[Agreement]       {
    // implicit val schema: Schema[Agreement] = Schema.derived
    // val id: Lens[Agreement, String]        = field(_.id)
  }
  final case class OpsAgreement(id: String, ops: String)                    extends Agreement
  object OpsAgreement                                                       extends CompanionOptics[OpsAgreement]    {
    implicit val schema: Schema[OpsAgreement] = Schema.derived
    val id: Lens[OpsAgreement, String]        = optic(_.id)
    val ops: Lens[OpsAgreement, String]       = optic(_.ops)
  }
  sealed trait DirectAgreement                                              extends Agreement                        {
    def id: String
    def identityId: String
  }
  object DirectAgreement                                                    extends CompanionOptics[DirectAgreement] {
//    implicit val schema: Schema[DirectAgreement] = Schema.derived
    // val id: Lens[DirectAgreement, String]        = field(_.id)
    // val identityId: Lens[DirectAgreement, String] = field(_.identityId)
  }
  final case class DirectOtpAgreement(id: String, identityId: String)       extends DirectAgreement
  final case class DirectRecurringAgreement(id: String, identityId: String) extends DirectAgreement

  // ================================================================================================

}
