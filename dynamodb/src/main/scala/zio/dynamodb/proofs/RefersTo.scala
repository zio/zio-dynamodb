package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that a value of type `A` can be used where type `X` is expected. An instance exists when `A` is `X` (and,
 * because `A` is contravariant, when `A` is a subtype of `X`), and when either type is `ProjectionExpression.Unknown`.
 */
@implicitNotFound("the type ${A} must be a ${X} in order to use this operator")
sealed trait RefersTo[X, -A]

/** Lower-priority instances of [[RefersTo]]. */
trait RefersToLowerPriorityImplicits0 extends RefersToLowerPriorityImplicits1 {

  /** Proves [[RefersTo]] for any `X` when `A` is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: RefersTo[X, ProjectionExpression.Unknown] =
    new RefersTo[X, ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[RefersTo]], for equal types. */
trait RefersToLowerPriorityImplicits1 {

  /** Proves [[RefersTo]] when both types are the same. */
  implicit def identity[A]: RefersTo[A, A] = new RefersTo[A, A] {}
}

/** Instances of [[RefersTo]]. */
object RefersTo extends RefersToLowerPriorityImplicits0 {

  /** Proves [[RefersTo]] for any `A` when `X` is `ProjectionExpression.Unknown`. */
  implicit def unknownLeft[X]: RefersTo[ProjectionExpression.Unknown, X] =
    new RefersTo[ProjectionExpression.Unknown, X] {}
}
