package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait RefersTo[X, -A]
trait RefersToLowerPriorityImplicits0 extends RefersToLowerPriorityImplicits1 {
  implicit def unknownRight[X]: RefersTo[X, ProjectionExpression.Unknown] =
    new RefersTo[X, ProjectionExpression.Unknown] {}
}
trait RefersToLowerPriorityImplicits1 {
  implicit def identity[A]: RefersTo[A, A] = new RefersTo[A, A] {}
}
object RefersTo                       extends RefersToLowerPriorityImplicits0 {
  implicit def unknownLeft[X]: RefersTo[ProjectionExpression.Unknown, X] =
    new RefersTo[ProjectionExpression.Unknown, X] {}
}
