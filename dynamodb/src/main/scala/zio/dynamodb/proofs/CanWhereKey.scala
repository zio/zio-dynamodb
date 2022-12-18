package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound

@implicitNotFound(
  "Mixed types for the key condition expression found - ${A}"
)
sealed trait CanWhereKey[A, -B]

object CanWhereKey {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanWhereKey[A, B] = {
    val _ = ev
    new CanWhereKey[A, B] {}
  }
  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanWhereKey[A, B]): CanWhereKey[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanWhereKey[A, Stream[Throwable, B]] {}
  }
}
