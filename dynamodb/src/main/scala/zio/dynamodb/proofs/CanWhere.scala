package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Mixed types for the condition expression found - ${A}"
)
sealed trait CanWhere[A, -B]

object CanWhere {
  implicit def subtypeCanWhere[A, B](implicit ev: B <:< A): CanWhere[A, B] = {
    val _ = ev
    new CanWhere[A, B] {}
  }

  implicit def subtypeCanWhereReturnOption[A, B](implicit ev: CanWhere[A, B]): CanWhere[A, Option[B]] = {
    val _ = ev
    new CanWhere[A, Option[B]] {}
  }

}
