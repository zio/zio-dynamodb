package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound
import zio.Chunk
import zio.dynamodb.LastEvaluatedKey

@implicitNotFound(
  "Mixed types for the key condition expression found - ${A}"
)
// TODO: Avi - delete - not used
sealed trait CanWhereKey[A, -B]

trait CanWhereKeyLowerPriorityImplicit {
  implicit def subtypeCanWhereKey[A, B](implicit ev: B <:< A): CanWhereKey[A, B] = {
    val _ = ev
    new CanWhereKey[A, B] {}
  }
}

object CanWhereKey extends CanWhereKeyLowerPriorityImplicit {

  implicit def scanAndQuerySomeCanWhereKey[A]: CanWhereKey[A, (Chunk[A], LastEvaluatedKey)] =
    new CanWhereKey[A, (Chunk[A], LastEvaluatedKey)] {}

  implicit def subtypeStreamCanWhereKey[A, B](implicit ev: CanWhereKey[A, B]): CanWhereKey[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanWhereKey[A, Stream[Throwable, B]] {}
  }
}
