package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support remove operations on the type ${X}. To use this operation work with any collection type that extends 'Seq'"
)
sealed trait ListRemoveable[-X]
trait ListRemoveable0 extends ListRemoveableLowPriorityImplicits {
  implicit def unknownRight[X]: ListRemoveable[ProjectionExpression.Unknown] =
    new ListRemoveable[ProjectionExpression.Unknown] {}
}
trait ListRemoveableLowPriorityImplicits {
  implicit def list[A]: ListRemoveable[Seq[A]] = new ListRemoveable[Seq[A]] {}
}
object ListRemoveable extends ListRemoveable0
