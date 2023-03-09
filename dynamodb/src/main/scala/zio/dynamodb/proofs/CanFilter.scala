package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound
import zio.Chunk
import zio.dynamodb.LastEvaluatedKey

@implicitNotFound(
  "Mixed types for the filter expression found - ${A}"
)
sealed trait CanFilter[A, -B]

trait CanFilterLowpriorityImplicits {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B] = {
    val _ = ev
    new CanFilter[A, B] {}
  }
}
object CanFilter extends CanFilterLowpriorityImplicits {
  implicit def scanAndQuerySomeCanFilter[A]: CanFilter[A, (Chunk[A], LastEvaluatedKey)] =
    new CanFilter[A, (Chunk[A], LastEvaluatedKey)] {}

  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanFilter[A, B]): CanFilter[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanFilter[A, Stream[Throwable, B]] {}
  }
}
