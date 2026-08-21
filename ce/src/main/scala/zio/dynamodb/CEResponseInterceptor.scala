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

import cats.effect.{ IO, IOLocal }
import zio.blocks.chunk.Chunk

/** Helpers for building Cats Effect-native [[ResponseInterceptor]] instances. */
object CEResponseInterceptor {

  /** Returned by [[accumulating]]. */
  final case class Accumulating(
    interceptor: ResponseInterceptor[IO],
    results: IO[Chunk[DynamoDBResponseMetadata]]
  )

  /**
   * Creates a [[ResponseInterceptor]] backed by an [[cats.effect.IOLocal]] that accumulates
   *  metadata in call order. `results` reads the accumulated chunk without
   *  consuming it.
   */
  def accumulating: IO[Accumulating] =
    IOLocal(List.empty[DynamoDBResponseMetadata]).map { local =>
      val interceptor                                       = new ResponseInterceptor[IO] {
        def onResponse(meta: DynamoDBResponseMetadata): IO[Unit] =
          local.update(meta :: _)
      }
      val readMetadata: IO[Chunk[DynamoDBResponseMetadata]] =
        local.get.map(xs => Chunk.fromIterable(xs.reverse))
      Accumulating(interceptor, readMetadata)
    }
}
