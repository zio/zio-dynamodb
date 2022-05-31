package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support the 'size' operation on type ${X}. This operation is only supported for collections that extends Iterable and String"
)
sealed trait Sizable[-X]
trait SizableLowPriorityImplicits0 extends SizableLowPriorityImplicits1 {
  implicit def unknown: Sizable[ProjectionExpression.Unknown] =
    new Sizable[ProjectionExpression.Unknown] {}
}
trait SizableLowPriorityImplicits1 {
  implicit def iterable[A]: Sizable[Iterable[A]] = new Sizable[Iterable[A]] {}
  implicit def string[A]: Sizable[String]        = new Sizable[String] {}
}
object Sizable                     extends SizableLowPriorityImplicits0 {} // TODO: shuffle up
