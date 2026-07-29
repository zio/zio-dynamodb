package zio.dynamodb.proofs

import scala.annotation.implicitNotFound
import zio.dynamodb.Page

@implicitNotFound(
  "DynamoDB only supports filter on scan and query operations on type ${B}"
)
sealed trait CanFilter[A, -B]

trait CanFilterLowpriorityImplicits {
  implicit def subtypeCanFilter[A, B](implicit ev: B <:< A): CanFilter[A, B] = {
    val _ = ev
    new CanFilter[A, B] {}
  }
}
object CanFilter extends CanFilterLowpriorityImplicits {
  implicit def pageCanFilter[A]: CanFilter[A, Page[A]] =
    new CanFilter[A, Page[A]] {}

  // Allows filter expressions typed to A on HL scan/query results whose items are Page[Either[E, A]].
  implicit def pageEitherCanFilter[E, A]: CanFilter[A, Page[Either[E, A]]] =
    new CanFilter[A, Page[Either[E, A]]] {}

//  implicit def subtypeStreamCanFilter[A, B](implicit ev: CanFilter[A, B]): CanFilter[A, Stream[Throwable, B]] = {
//    val _ = ev
//    new CanFilter[A, Stream[Throwable, B]] {}
//  }
}
