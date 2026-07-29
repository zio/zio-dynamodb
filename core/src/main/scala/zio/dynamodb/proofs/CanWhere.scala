package zio.dynamodb.proofs

import scala.annotation.implicitNotFound

@implicitNotFound(
  "DynamoDB only supports conditions expressions on put, update and delete operations. " +
    "Furthermore query output type ${B} must match condition expression type ${A}"
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
