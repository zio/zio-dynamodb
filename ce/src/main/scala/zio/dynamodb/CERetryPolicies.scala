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

import cats.effect.{ Async, Ref }

import scala.concurrent.duration.FiniteDuration

/**
 * Cats-Effect-specific [[EffectfulRetryPolicy]] smart constructors, generic over any
 * `F[_]: Async` (not just `cats.effect.IO`). Unlike ZIO, cats-effect has no built-in
 * `Schedule`-equivalent to wrap, so this is the direct CE-native way to get the same
 * state-scoping guarantee `ZioRetryPolicies.fromSchedule` gives ZIO users: state lives in a
 * `Ref[F, S]`, created fresh once per `newAttempt()` call, so concurrent executions sharing
 * one policy instance never share state.
 */
object CERetryPolicies {

  /**
   * A stateful curve keyed by an arbitrary state type `S` (e.g. `Long` for decorrelated
   * jitter's "previous delay"). `next` computes the new state and the delay for this attempt
   * from the current state; returning `None` stops retrying.
   *
   * {{{
   * // decorrelated jitter — sleep = min(cap, random_between(base, previous_sleep * 3))
   * CERetryPolicies.statefulCustom[IO](initial = 100L) { (previousDelay, attempt) =>
   *   if (attempt >= 8) (previousDelay, None)
   *   else {
   *     val next = math.min(20000L, 100L + scala.util.Random.between(0L, previousDelay * 3 - 100L + 1))
   *     (next, Some(FiniteDuration(next, "milliseconds")))
   *   }
   * }
   * }}}
   */
  def statefulCustom[F[_], S](
    initial: S
  )(next: (S, Int) => (S, Option[FiniteDuration]))(implicit F: Async[F]): EffectfulRetryPolicy[F] =
    new EffectfulRetryPolicy[F] {
      def newAttempt(): F[EffectfulRetryPolicy.Attempt[F]] =
        F.map(Ref.of[F, S](initial)) { stateRef =>
          new EffectfulRetryPolicy.Attempt[F] {
            def nextDelay(attempt: Int): F[Option[FiniteDuration]] =
              stateRef.modify(s => next(s, attempt))
          }
        }
    }

  /**
   * Full-jitter exponential backoff — see `RetryPolicy.fullJitter` for the formula, and why
   * it's what AWS SDKs actually ship as their default today. Stateless, so no `Ref` is needed;
   * the wrapping `Attempt` is built once here (not per `newAttempt()` call) and shared, since
   * it carries no per-execution state of its own to isolate.
   */
  def fullJitter[F[_]](
    maxRetries: Int = 8,
    baseDelay: FiniteDuration = FiniteDuration(100, "milliseconds"),
    maxDelay: FiniteDuration = FiniteDuration(20, "seconds")
  )(implicit F: Async[F]): EffectfulRetryPolicy[F] = {
    val pureAttempt   = RetryPolicy.fullJitter(maxRetries, baseDelay, maxDelay).newAttempt()
    val cachedAttempt = new EffectfulRetryPolicy.Attempt[F] {
      def nextDelay(attempt: Int): F[Option[FiniteDuration]] = F.delay(pureAttempt.nextDelay(attempt))
    }
    new EffectfulRetryPolicy[F] {
      def newAttempt(): F[EffectfulRetryPolicy.Attempt[F]] = F.pure(cachedAttempt)
    }
  }
}
