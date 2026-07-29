/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
