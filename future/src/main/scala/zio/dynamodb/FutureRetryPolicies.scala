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
import scala.concurrent.duration.FiniteDuration

/**
 * Future-specific [[EffectfulRetryPolicy]] smart constructors. `scala.concurrent.Future` has no
 * `Ref`/STM-equivalent, so state lives in a plain `var` closed over inside `newAttempt()` —
 * safe because one execution's `nextDelay` calls never overlap (each waits for the previous
 * to complete before the next is made), so nothing else can observe the closure mid-mutation.
 */
object FutureRetryPolicies {

  /**
   * A stateful curve keyed by an arbitrary state type `S` (e.g. `Long` for decorrelated
   * jitter's "previous delay"). `next` computes the new state and the delay for this attempt
   * from the current state; returning `None` stops retrying.
   */
  def statefulCustom[S](initial: S)(next: (S, Int) => (S, Option[FiniteDuration])): EffectfulRetryPolicy[Future] =
    new EffectfulRetryPolicy[Future] {
      def newAttempt(): Future[EffectfulRetryPolicy.Attempt[Future]] =
        Future.successful {
          var state = initial
          new EffectfulRetryPolicy.Attempt[Future] {
            def nextDelay(attempt: Int): Future[Option[FiniteDuration]] = {
              val (newState, delay) = next(state, attempt)
              state = newState
              Future.successful(delay)
            }
          }
        }
    }

  /**
   * AWS's own recommended decorrelated-jitter algorithm — see `RetryPolicy.awsRecommended`
   * for the shared formula.
   */
  def awsRecommended(
    maxRetries: Int = 8,
    baseDelay: FiniteDuration = FiniteDuration(100, "milliseconds"),
    maxDelay: FiniteDuration = FiniteDuration(20, "seconds")
  ): EffectfulRetryPolicy[Future] =
    statefulCustom(initial = baseDelay.toMillis) { (previousDelayMs, attempt) =>
      RetryPolicy.decorrelatedJitterStep(baseDelay.toMillis, maxDelay.toMillis, maxRetries)(previousDelayMs, attempt)
    }
}
