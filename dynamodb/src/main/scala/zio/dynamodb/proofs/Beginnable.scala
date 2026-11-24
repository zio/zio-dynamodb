package zio.dynamodb.proofs

import zio.dynamodb.ProjectionExpression

import scala.annotation.implicitNotFound

/**
 * Evidence that `beginsWith` can be applied to an attribute of type `A` with a prefix of type `X`. Instances exist for
 * a `String` prefix with a `String` or `Option[String]` attribute, and when either type is
 * `ProjectionExpression.Unknown`.
 */
@implicitNotFound(
  "DynamoDB does not support 'beginsWith' on type ${A}. This operator only applies to String and Option[String]"
)
sealed trait Beginnable[X, -A]

/** Lower-priority instances of [[Beginnable]]. */
trait BeginnableLowPriorityImplicits0 extends BeginnableLowPriorityImplicits1 {

  /** Proves `beginsWith` for any prefix type `X` when the attribute type is `ProjectionExpression.Unknown`. */
  implicit def unknownRight[X]: Beginnable[X, ProjectionExpression.Unknown] =
    new Beginnable[X, ProjectionExpression.Unknown] {}
}

/** Lowest-priority instances of [[Beginnable]], for strings. */
trait BeginnableLowPriorityImplicits1 {

  /** Proves `beginsWith` for a `String` attribute. */
  implicit def string: Beginnable[String, String] = new Beginnable[String, String] {}

  /** Proves `beginsWith` for an `Option[String]` attribute. */
  implicit def optString: Beginnable[String, Option[String]] = new Beginnable[String, Option[String]] {}
}

/** Instances of [[Beginnable]]. */
object Beginnable extends BeginnableLowPriorityImplicits0 {

  /** Proves `beginsWith` for any attribute type `X` when the prefix type is `ProjectionExpression.Unknown`. */
  implicit def unknownLeft[X]: Beginnable[ProjectionExpression.Unknown, X] =
    new Beginnable[ProjectionExpression.Unknown, X] {}
}
