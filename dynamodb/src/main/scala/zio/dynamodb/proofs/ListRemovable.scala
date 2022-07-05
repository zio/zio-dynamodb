package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support remove operations on the type ${X}. To use this operation work with any collection type that extends 'Seq'"
)
sealed trait ListRemovable[-X]
trait ListRemoveable0 extends ListRemoveableLowPriorityImplicits {
  implicit def unknownRight[X]: ListRemovable[ProjectionExpression.Unknown] =
    new ListRemovable[ProjectionExpression.Unknown] {}
}
trait ListRemoveableLowPriorityImplicits {
  implicit def list[A]: ListRemovable[Seq[A]] = new ListRemovable[Seq[A]] {}
}
object ListRemovable  extends ListRemoveable0
