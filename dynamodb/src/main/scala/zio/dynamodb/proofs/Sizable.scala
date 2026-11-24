package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that the `size` condition operand can be applied to an attribute of type `X`. Instances exist for `String`,
 * for `Iterable[A]` (so for `List`, `Set` and `Map` among others) and for `ProjectionExpression.Unknown`.
 * `ProjectionExpression#size` asks for it.
 */
@implicitNotFound(
  "DynamoDB does not support the 'size' operation on type ${X}. This operation is only supported for collections that extends Iterable and String"
)
sealed trait Sizable[-X]

/** Lower-priority instances of [[Sizable]]. */
trait SizableLowPriorityImplicits0 extends SizableLowPriorityImplicits1 {

  /** Proves `size` for an attribute of type `ProjectionExpression.Unknown`. */
  implicit def unknown: Sizable[ProjectionExpression.Unknown] =
    new Sizable[ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[Sizable]], for collections and strings. */
trait SizableLowPriorityImplicits1 {

  /** Proves `size` for an `Iterable[A]` attribute. */
  implicit def iterable[A]: Sizable[Iterable[A]] = new Sizable[Iterable[A]] {}

  /** Proves `size` for a `String` attribute. */
  implicit def string[A]: Sizable[String] = new Sizable[String] {}
}

/** Instances of [[Sizable]]. */
object Sizable extends SizableLowPriorityImplicits0
