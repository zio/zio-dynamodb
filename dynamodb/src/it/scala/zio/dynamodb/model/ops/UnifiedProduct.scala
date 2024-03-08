package zio.dynamodb.model.ops

import zio.schema.DeriveSchema

import scala.annotation.nowarn

final case class UnifiedProduct(
    productId: String,
    phaseId: String,
    campaignId: String
)
object UnifiedProduct {

  @nowarn
  implicit val schema = DeriveSchema.gen[UnifiedProduct]
}
