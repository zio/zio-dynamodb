package zio.dynamodb.examples.model

import zio.dynamodb.ProjectionExpression
import zio.schema.DeriveSchema

final case class Elephant(id: String, email: String)

object Elephant {
  implicit val schema = DeriveSchema.gen[Elephant]
  val (id, email)     = ProjectionExpression.accessors[Elephant]
}
