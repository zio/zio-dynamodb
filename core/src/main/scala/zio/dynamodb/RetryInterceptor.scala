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
 * Callback invoked just before an effect-level retry sleeps and retries — the retry loop shared
 *  by single-item operations and batch operations alike. A retried-then-succeeded attempt is
 *  otherwise invisible to the caller: [[ResponseInterceptor]] only fires on the response that
 *  ultimately succeeds, never on the failed attempts that preceded it.
 *
 *  Separate from [[ResponseInterceptor]] since most `ResponseInterceptor` implementors have no
 *  interest in retries; separate from [[BatchRetryInterceptor]] since batch's response-level
 *  resubmission loop (unprocessed keys/items, no `Throwable` involved) is a genuinely different
 *  mechanism this trait doesn't cover.
 */
trait RetryInterceptor[F[_]] {

  /**
   * Called once per retried attempt, right before the retry delay is slept. `attempt` is the
   *  0-indexed attempt number that just failed (0 = the first call failed).
   */
  def onRetry(meta: DynamoDBRetryMetadata, error: Throwable, attempt: Int): F[Unit]
}
