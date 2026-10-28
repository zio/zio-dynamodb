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
 * A client-wide circuit breaker on retries, like the AWS SDK's own retry quota. Unlike a
 * [[RetryPolicy]], it is **shared**: one budget per interpreter, used by every concurrent call,
 * because "has this client been failing a lot lately" only means something in aggregate.
 *
 * Each retry debits a cost (5 for a throttling error, 14 for other transient errors - see
 * `RetryPolicy.retryCost`); a call that succeeds without retrying credits back 1, and one that
 * succeeds after retrying credits back what its retries cost. When the budget runs out, a failed
 * call fails without retrying, and no `onRetry` fires.
 */
trait RetryQuota[F[_]] {

  /** True and debited if there's enough budget to retry; false (no debit) if exhausted. */
  def tryConsume(cost: Int): F[Boolean]

  /** Restores tokens on eventual success — `amount` is what this execution's own retries cost. */
  def credit(amount: Int): F[Unit]
}
