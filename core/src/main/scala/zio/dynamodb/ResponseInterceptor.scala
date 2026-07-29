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

/**
 * Callback invoked after each AWS DynamoDB operation completes. The returned
 *  `F[Unit]` is sequenced into the interpreter before the caller receives the
 *  result, so side effects (logging, metrics, accumulation) are guaranteed to run.
 *
 *  Implementations must be thread-safe when shared across concurrent requests.
 *  Use [[ZioResponseInterceptor.accumulating]], [[CEResponseInterceptor.accumulating]],
 *  or [[FutureResponseInterceptor.accumulating]] for a ready-made accumulator.
 */
trait ResponseInterceptor[F[_]] {

  /**
   * Called after each DynamoDB data operation completes successfully.
   *  The effect is sequenced before the result is returned to the caller.
   */
  def onResponse(meta: DynamoDBResponseMetadata): F[Unit]
}
