package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

sealed trait CanSortKeyBeginsWith[X, -A]
trait CanSortKeyBeginsWith0 extends CanSortKeyBeginsWith1 {
  implicit def unknownRight[X]: CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] =
    new CanSortKeyBeginsWith[X, ProjectionExpression.Unknown] {}
}
trait CanSortKeyBeginsWith1 {
  implicit def bytes[A]: CanSortKeyBeginsWith[Iterable[Byte], Iterable[Byte]] =
    new CanSortKeyBeginsWith[Iterable[Byte], Iterable[Byte]] {}
  implicit def bytes2[A]: CanSortKeyBeginsWith[Array[Byte], Array[Byte]]      =
    new CanSortKeyBeginsWith[Array[Byte], Array[Byte]] {}
  implicit def bytes3[A]: CanSortKeyBeginsWith[List[Byte], List[Byte]]        =
    new CanSortKeyBeginsWith[List[Byte], List[Byte]] {}
  // TODO: Avi - other collection types

  implicit def string: CanSortKeyBeginsWith[String, String]                   = new CanSortKeyBeginsWith[String, String] {}
}
object CanSortKeyBeginsWith extends CanSortKeyBeginsWith0 {
  implicit def unknownLeft[X]: CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] =
    new CanSortKeyBeginsWith[ProjectionExpression.Unknown, X] {}
}
