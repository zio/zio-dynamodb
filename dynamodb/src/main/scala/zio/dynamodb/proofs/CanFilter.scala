package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound

@implicitNotFound(
  "Mixed types for the filter expression found - ${A}"
)
sealed trait CanFilter[A, -B]

// create lowPriorityCanFilter, prefer Stream one
object CanFilter {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B] = {
    val _ = ev
    new CanFilter[A, B] {}
  }
  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanFilter[A, B]): CanFilter[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanFilter[A, Stream[Throwable, B]] {}
  }
}
