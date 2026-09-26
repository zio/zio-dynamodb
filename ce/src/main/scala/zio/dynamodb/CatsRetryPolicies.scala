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

import cats.effect.{ IO, Ref }

import scala.concurrent.duration.FiniteDuration

/**
 * Cats-Effect-specific [[EffectfulRetryPolicy]] smart constructors. Unlike ZIO, cats-effect has
 * no built-in `Schedule`-equivalent to wrap, so this is the direct CE-native way to get the same
 * state-scoping guarantee `ZioRetryPolicies.fromSchedule` gives ZIO users: state lives in a
 * `Ref[IO, S]`, created fresh once per `newAttempt()` call, so concurrent executions sharing
 * one policy instance never share state.
 */
object CatsRetryPolicies {

  /**
   * A stateful curve keyed by an arbitrary state type `S` (e.g. `Long` for decorrelated
   * jitter's "previous delay"). `next` computes the new state and the delay for this attempt
   * from the current state; returning `None` stops retrying.
   *
   * {{{
   * // decorrelated jitter — AWS's own recommended algorithm,
   * // sleep = min(cap, random_between(base, previous_sleep * 3))
   * CatsRetryPolicies.statefulCustom(initial = 100L) { (previousDelay, attempt) =>
   *   if (attempt >= 8) (previousDelay, None)
   *   else {
   *     val next = math.min(20000L, 100L + scala.util.Random.between(0L, previousDelay * 3 - 100L + 1))
   *     (next, Some(FiniteDuration(next, "milliseconds")))
   *   }
   * }
   * }}}
   */
  def statefulCustom[S](
    initial: S
  )(next: (S, Int) => (S, Option[FiniteDuration])): EffectfulRetryPolicy[IO] =
    new EffectfulRetryPolicy[IO] {
      def newAttempt(): IO[EffectfulRetryPolicy.Attempt[IO]] =
        Ref.of[IO, S](initial).map { stateRef =>
          new EffectfulRetryPolicy.Attempt[IO] {
            def nextDelay(attempt: Int): IO[Option[FiniteDuration]] =
              stateRef.modify(s => next(s, attempt))
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
  ): EffectfulRetryPolicy[IO] =
    statefulCustom(initial = baseDelay.toMillis) { (previousDelayMs, attempt) =>
      RetryPolicy.decorrelatedJitterStep(baseDelay.toMillis, maxDelay.toMillis, maxRetries)(previousDelayMs, attempt)
    }
}
