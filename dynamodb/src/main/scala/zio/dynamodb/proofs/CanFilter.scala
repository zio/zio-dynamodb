package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound
import zio.dynamodb.DynamoDBError
import zio.dynamodb.LastEvaluatedKey
import zio.Chunk

@implicitNotFound(
  "Mixed types for the filter expression found - ${A}"
)
sealed trait CanFilter[A, -B]

trait CanFilterLowpriorityImplicits0 extends CanFilterLowpriorityImplicits1 {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B] = {
    val _ = ev
    new CanFilter[A, B] {}
  }
}
trait CanFilterLowpriorityImplicits1 {
  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanFilter[A, B]): CanFilter[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanFilter[A, Stream[Throwable, B]] {}
  }
}
// create lowPriorityCanFilter, prefer Stream one
object CanFilter                     extends CanFilterLowpriorityImplicits0 {
  implicit def subtypeEitherCanFilter[A](implicit
    ev: CanFilter[A, _]
  ): CanFilter[A, Either[DynamoDBError, (Chunk[A], LastEvaluatedKey)]] = {
    val _ = ev
    new CanFilter[A, Either[DynamoDBError, (Chunk[A], LastEvaluatedKey)]] {}
  }
}
/*
@implicitNotFound("DynamoDB does not support 'contains' on type ${A}. This operator only applies to sets and strings")
sealed trait Containable[X, -A]
trait ContainableLowPriorityImplicits0 extends ContainableLowPriorityImplicits1 {
  implicit def unknownRight[X]: Containable[X, ProjectionExpression.Unknown] =
    new Containable[X, ProjectionExpression.Unknown] {}
}
trait ContainableLowPriorityImplicits1 {
  implicit def set[A]: Containable[Set[A], A]      = new Containable[Set[A], A] {}
  implicit def string: Containable[String, String] = new Containable[String, String] {}
}
object Containable                     extends ContainableLowPriorityImplicits0 {
  implicit def unknownLeft[X]: Containable[ProjectionExpression.Unknown, X] =
    new Containable[ProjectionExpression.Unknown, X] {}
}
 */
