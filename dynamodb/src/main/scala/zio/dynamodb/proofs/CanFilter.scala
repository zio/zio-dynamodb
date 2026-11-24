package zio.dynamodb.proofs

import zio.stream.Stream
import scala.annotation.implicitNotFound
import zio.Chunk
import zio.dynamodb.LastEvaluatedKey

/**
 * Evidence that a filter expression on `A` can be added to a query whose result type is `B`. Instances exist when `B`
 * is a subtype of `A`, when `B` is `(Chunk[A], LastEvaluatedKey)` (the result of `scanSome` and `querySome`), and when
 * `B` is a `Stream[Throwable, B2]` and `B2` has an instance. `DynamoDBQuery#filter` asks for it.
 */
@implicitNotFound(
  "DynamoDB only supports filter on scan and query operations on type ${B}"
)
sealed trait CanFilter[A, -B]

/** Lower-priority instances of [[CanFilter]]. */
trait CanFilterLowpriorityImplicits {

  /** Proves the filter for a result type `B` that is a subtype of `A`. */
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B] = {
    val _ = ev
    new CanFilter[A, B] {}
  }
}

/** Instances of [[CanFilter]]. */
object CanFilter extends CanFilterLowpriorityImplicits {

  /** Proves the filter for the `(Chunk[A], LastEvaluatedKey)` result of a paged scan or query. */
  implicit def scanAndQuerySomeCanFilter[A]: CanFilter[A, (Chunk[A], LastEvaluatedKey)] =
    new CanFilter[A, (Chunk[A], LastEvaluatedKey)] {}

  /** Proves the filter for a `Stream[Throwable, B]` result when `B` has an instance. */
  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanFilter[A, B]): CanFilter[A, Stream[Throwable, B]] = {
    val _ = ev
    new CanFilter[A, Stream[Throwable, B]] {}
  }
}
