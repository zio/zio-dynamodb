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

package zio.dynamodb

import scala.concurrent.Future
import zio.blocks.chunk.Chunk
import java.util.concurrent.atomic.AtomicReference

/** Helpers for building [[scala.concurrent.Future]]-native [[ResponseInterceptor]] instances. */
object FutureResponseInterceptor {

  /** Returned by [[accumulating]]. */
  final case class Accumulating(
    interceptor: ResponseInterceptor[Future],
    results: () => Chunk[DynamoDBResponseMetadata]
  )

  /**
   * Creates a [[ResponseInterceptor]] backed by an [[java.util.concurrent.atomic.AtomicReference]] that accumulates
   *  metadata in call order.  `results` returns a snapshot of all entries collected
   *  so far.
   *
   *  Unlike [[ZioResponseInterceptor]] and `CEResponseInterceptor`, there is no
   *  fiber-local scoping — all concurrent callers that hold the same interceptor
   *  instance append to the same list.  Create a fresh interceptor per logical
   *  request to isolate metadata collection.
   *
   *  Call `results` only after all futures you care about have completed.
   */
  def accumulating: Future[Accumulating] = {
    val ref                                                 = new AtomicReference(List.empty[DynamoDBResponseMetadata])
    val interceptor                                         = new ResponseInterceptor[Future] {
      def onResponse(meta: DynamoDBResponseMetadata): Future[Unit] = {
        ref.updateAndGet(meta :: _)
        Future.successful(())
      }
    }
    val readMetadata: () => Chunk[DynamoDBResponseMetadata] =
      () => Chunk.fromIterable(ref.get.reverse)
    Future.successful(Accumulating(interceptor, readMetadata))
  }
}
