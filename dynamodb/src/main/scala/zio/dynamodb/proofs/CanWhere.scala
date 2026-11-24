package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

/**
 * Evidence that a condition expression on `A` can be added to a query whose result type is `B`. Instances exist when
 * `B` is a subtype of `A`, and when `B` is `Option[B2]` for a type that has an instance. `DynamoDBQuery#where` asks for
 * it.
 */
@implicitNotFound(
  "DynamoDB only supports conditions expressions on put, update and delete operations. " +
    "Furthermore query output type ${B} must match condition expression type ${A}"
)
sealed trait CanWhere[A, -B]

/** Instances of [[CanWhere]]. */
object CanWhere {

  /** Proves the condition for a result type `B` that is a subtype of `A`. */
  implicit def subtypeCanWhere[A, B](implicit ev: B <:< A): CanWhere[A, B] = {
    val _ = ev
    new CanWhere[A, B] {}
  }

  /** Proves the condition for an `Option[B]` result when `B` has an instance. */
  implicit def subtypeCanWhereReturnOption[A, B](implicit ev: CanWhere[A, B]): CanWhere[A, Option[B]] = {
    val _ = ev
    new CanWhere[A, Option[B]] {}
  }

}
