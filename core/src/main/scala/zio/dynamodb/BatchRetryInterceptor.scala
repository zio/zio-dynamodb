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

import zio.blocks.chunk.Chunk

/**
 * Callback invoked just before `batchGetItem`/`batchWriteItem`'s response-level resubmission
 *  loop sleeps and resubmits unprocessed keys/items — a **successful** response reporting AWS
 *  partial-failure detail, not a thrown error, so [[RetryInterceptor]] never sees it. Likely the
 *  more common batch retry signal in practice: DynamoDB throttling on batch ops typically shows
 *  up as partial `unprocessedItems`, not an exception.
 *
 *  `onBatchWriteRetry`'s puts/deletes are `Chunk`, not `Set`, deliberately — order-preserving,
 *  matching AWS's own `List<WriteRequest>` per table, so the interceptor doesn't reintroduce the
 *  silent-collapse behavior the underlying batch model was fixed to remove.
 */
trait BatchRetryInterceptor[F[_]] {

  /** Called once per response-level resubmission of `batchGetItem`, right before the delay. */
  def onBatchGetRetry(unprocessedKeys: Map[String, Set[PrimaryKey]], attempt: Int): F[Unit]

  /** Called once per response-level resubmission of `batchWriteItem`, right before the delay. */
  def onBatchWriteRetry(
    unprocessedPuts: Map[String, Chunk[Item]],
    unprocessedDeletes: Map[String, Chunk[PrimaryKey]],
    attempt: Int
  ): F[Unit]
}
