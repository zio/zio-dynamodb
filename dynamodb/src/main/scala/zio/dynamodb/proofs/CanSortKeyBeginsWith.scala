package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression
import scala.annotation.implicitNotFound

@implicitNotFound(
  "Field of type ${X} has 'beginsWith' argument of type ${A} - they must be the same type and be a string or a byte array"
)
sealed trait CanSortKeyBeginsWith[-X, -A]
trait CanSortKeyBeginsWith0 extends CanSortKeyBeginsWith1 {
  implicit def unknownRight[X]: CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] =
    new CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] {}
}
trait CanSortKeyBeginsWith1 {
  // begins_with with only applies to keys of type string or bytes
  implicit def bytes[A <: Iterable[Byte]]: CanSortKeyBeginsWith[A, A] =
    new CanSortKeyBeginsWith[A, A] {}
  implicit def string: CanSortKeyBeginsWith[String, String]           = new CanSortKeyBeginsWith[String, String] {}
}
object CanSortKeyBeginsWith extends CanSortKeyBeginsWith0 {
  implicit def unknownLeft[X]: CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] =
    new CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] {}
}
