package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support 'beginsWith' on type ${A}. This operator only applies to String and Option[String]"
)
sealed trait Beginnable[X, -A]
trait BeginnableLowPriorityImplicits0 extends BeginnableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Beginnable[X, ProjectionExpression.Unknown] =
    new Beginnable[X, ProjectionExpression.Unknown] {}
}
trait BeginnableLowPriorityImplicits1 {
  implicit def string: Beginnable[String, String]            = new Beginnable[String, String] {}
  implicit def optString: Beginnable[String, Option[String]] = new Beginnable[String, Option[String]] {}
}
object Beginnable                     extends BeginnableLowPriorityImplicits0 {
  implicit def unknownLeft[X]: Beginnable[ProjectionExpression.Unknown, X] =
    new Beginnable[ProjectionExpression.Unknown, X] {}
}
