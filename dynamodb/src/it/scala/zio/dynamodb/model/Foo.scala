package zio.dynamodb.model

import zio.dynamodb.model.ops.UnifiedOffer
import zio.dynamodb.ProjectionExpression

final case class Foo(id: String, offer: Option[UnifiedOffer])
object Foo {
  implicit val schema = zio.schema.DeriveSchema.gen[Foo]
  val (id, offerId)   = ProjectionExpression.accessors[Foo]
}
