package zio.dynamodb.model.ops

import zio.Chunk
import zio.schema.DeriveSchema

import scala.annotation.nowarn

final case class UnifiedOffer(offerId: String, products: Chunk[UnifiedProduct])
object UnifiedOffer {

  @nowarn
  implicit val schema = DeriveSchema.gen[UnifiedOffer]
}
