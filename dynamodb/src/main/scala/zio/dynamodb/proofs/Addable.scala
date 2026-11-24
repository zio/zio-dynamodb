package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that the `add` update action can be applied to an attribute of type `X` with an operand of type `A`.
 * Instances exist for `Set[A]` with `A`, for `Int`, `Long`, `Float`, `Double` and `Short` with the same type, and when
 * either type is `ProjectionExpression.Unknown`. `ProjectionExpression#add` asks for `Addable[To, To]` and `addSet`
 * asks for `Addable[To, A]`.
 */
@implicitNotFound(
  "DynamoDB does not support 'add' operations on the type ${A}. This operation is only supported for sets and numerics"
)
sealed trait Addable[X, -A]

/** Lower-priority instances of [[Addable]]. */
trait AddableLowPriorityImplicits0 extends AddableLowPriorityImplicits1 {

  /** Proves `add` for any `X` when the operand type is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: Addable[X, ProjectionExpression.Unknown] =
    new Addable[X, ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[Addable]], for sets and numbers. */
trait AddableLowPriorityImplicits1 {

  /** Proves `add` for a `Set[A]` attribute with an operand of type `A`. */
  implicit def set[A]: Addable[Set[A], A] = new Addable[Set[A], A] {}

  /** Proves `add` for an `Int` attribute. */
  implicit def int: Addable[Int, Int] = new Addable[Int, Int] {}

  /** Proves `add` for a `Long` attribute. */
  implicit def long: Addable[Long, Long] = new Addable[Long, Long] {}

  /** Proves `add` for a `Float` attribute. */
  implicit def float: Addable[Float, Float] = new Addable[Float, Float] {}

  /** Proves `add` for a `Double` attribute. */
  implicit def double: Addable[Double, Double] = new Addable[Double, Double] {}

  /** Proves `add` for a `Short` attribute. */
  implicit def short: Addable[Short, Short] = new Addable[Short, Short] {}
}

/** Instances of [[Addable]]. */
object Addable extends AddableLowPriorityImplicits0 {

  /** Proves `add` for any operand type `X` when the attribute type is `ProjectionExpression.Unknown`. */
  implicit def unknownLeft[X]: Addable[ProjectionExpression.Unknown, X] =
    new Addable[ProjectionExpression.Unknown, X] {}
}
