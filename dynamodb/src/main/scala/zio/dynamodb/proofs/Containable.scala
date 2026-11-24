package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that `contains` can be applied to an attribute of type `X` with an element of type `A`. Instances exist for
 * `Set[A]`, `List[A]`, `String` and `Option[String]` (with a `String` element), and when either type is
 * `ProjectionExpression.Unknown`.
 */
@implicitNotFound(
  "DynamoDB does not support [${X}].contains([${A}]). This operator only applies to Sets, Lists, String and Option[String]"
)
sealed trait Containable[X, -A]

/** Lower-priority instances of [[Containable]]. */
trait ContainableLowPriorityImplicits0 extends ContainableLowPriorityImplicits1 {

  /** Proves `contains` for any `X` when the element type is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: Containable[X, ProjectionExpression.Unknown] =
    new Containable[X, ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[Containable]], for sets, lists and strings. */
trait ContainableLowPriorityImplicits1 {

  /** Proves `contains` for a `Set[A]` attribute. */
  implicit def set[A]: Containable[Set[A], A] = new Containable[Set[A], A] {}

  /** Proves `contains` for a `List[A]` attribute. */
  implicit def list[A]: Containable[List[A], A] = new Containable[List[A], A] {}

  /** Proves `contains` for a `String` attribute with a `String` element. */
  implicit def string: Containable[String, String] = new Containable[String, String] {}

  /** Proves `contains` for an `Option[String]` attribute with a `String` element. */
  implicit def optString: Containable[Option[String], String] = new Containable[Option[String], String] {}
}

/** Instances of [[Containable]]. */
object Containable extends ContainableLowPriorityImplicits0 {

  /** Proves `contains` for any element type `X` when the attribute type is `ProjectionExpression.Unknown`. */
  implicit def unknownLeft[X]: Containable[ProjectionExpression.Unknown, X] =
    new Containable[ProjectionExpression.Unknown, X] {}
}
