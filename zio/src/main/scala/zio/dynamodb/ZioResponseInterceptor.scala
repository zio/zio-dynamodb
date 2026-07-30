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

import zio._
import zio.blocks.chunk.Chunk

/** Helpers for building ZIO-native [[ResponseInterceptor]] instances. */
object ZioResponseInterceptor {

  /** Returned by [[accumulating]]. */
  final case class Accumulating(
    interceptor: ResponseInterceptor[Task],
    results: UIO[Chunk[DynamoDBResponseMetadata]]
  )

  /**
   * Creates a [[ResponseInterceptor]] backed by a [[Ref]] that accumulates
   *  metadata in call order. `results` reads the accumulated chunk without
   *  consuming it.
   *
   *  The accumulator is shared by any fiber that holds a reference to the
   *  returned interceptor. Create a fresh interceptor per request to isolate
   *  metadata collection.
   */
  def accumulating: UIO[Accumulating] =
    Ref.make(List.empty[DynamoDBResponseMetadata]).map { ref =>
      val interceptor                                        = new ResponseInterceptor[Task] {
        def onResponse(meta: DynamoDBResponseMetadata): Task[Unit] =
          ref.update(meta :: _)
      }
      val readMetadata: UIO[Chunk[DynamoDBResponseMetadata]] =
        ref.get.map(xs => Chunk.fromIterable(xs.reverse))
      Accumulating(interceptor, readMetadata)
    }
}
