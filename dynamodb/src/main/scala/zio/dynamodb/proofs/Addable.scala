package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB does not support 'add' operations on the type ${A}. This operation is only supported for sets and numerics"
)
sealed trait Addable[X, -A]
trait AddableLowPriorityImplicits0 extends AddableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Addable[X, ProjectionExpression.Unknown] =
    new Addable[X, ProjectionExpression.Unknown] {}
}
trait AddableLowPriorityImplicits1 {
  implicit def set[A]: Addable[Set[A], A]      = new Addable[Set[A], A] {}
  implicit def int: Addable[Int, Int]          = new Addable[Int, Int] {}
  implicit def long: Addable[Long, Long]       = new Addable[Long, Long] {}
  implicit def float: Addable[Float, Float]    = new Addable[Float, Float] {}
  implicit def double: Addable[Double, Double] = new Addable[Double, Double] {}
  implicit def short: Addable[Short, Short]    = new Addable[Short, Short] {}
}
object Addable                     extends AddableLowPriorityImplicits0 {
  implicit def unknownLeft[X]: Addable[ProjectionExpression.Unknown, X] =
    new Addable[ProjectionExpression.Unknown, X] {}
}
