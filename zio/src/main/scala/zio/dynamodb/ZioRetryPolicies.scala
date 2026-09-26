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

import scala.concurrent.duration.FiniteDuration

/** ZIO-specific [[EffectfulRetryPolicy]] smart constructors. */
object ZioRetryPolicies {

  /**
   * Wraps a `zio.Schedule` as an [[EffectfulRetryPolicy]]. Drives `Schedule#step` directly,
   * not `Schedule#driver`/`Driver.next` — the latter bakes its own `ZIO.sleep` into `next`,
   * which would double-sleep alongside `AwsInterpreter`'s own `sleep(d)` call. State lives in a
   * `Ref`, created fresh once per `newAttempt()` call, so concurrent executions sharing one
   * policy instance never share state.
   */
  def fromSchedule[Out](schedule: Schedule[Any, Unit, Out]): EffectfulRetryPolicy[Task] =
    new EffectfulRetryPolicy[Task] {
      def newAttempt(): Task[EffectfulRetryPolicy.Attempt[Task]] =
        Ref.make(schedule.initial).map { stateRef =>
          new EffectfulRetryPolicy.Attempt[Task] {
            def nextDelay(attempt: Int): Task[Option[FiniteDuration]] =
              for {
                state      <- stateRef.get
                now        <- Clock.currentDateTime
                stepResult <- schedule.step(now, (), state)
                (next, _, dec) = stepResult
                _          <- stateRef.set(next)
              } yield dec match {
                case Schedule.Decision.Continue(intervals) =>
                  Some(FiniteDuration(java.time.Duration.between(now, intervals.start).toMillis, "milliseconds"))
                case Schedule.Decision.Done                =>
                  None
              }
          }
        }
    }

  /**
   * AWS's own recommended decorrelated-jitter algorithm — see `RetryPolicy.awsRecommended`
   * for the shared formula. State lives in a `Ref`, created fresh once per `newAttempt()` call.
   */
  def awsRecommended(
    maxRetries: Int = 8,
    baseDelay: FiniteDuration = FiniteDuration(100, "milliseconds"),
    maxDelay: FiniteDuration = FiniteDuration(20, "seconds")
  ): EffectfulRetryPolicy[Task] =
    new EffectfulRetryPolicy[Task] {
      def newAttempt(): Task[EffectfulRetryPolicy.Attempt[Task]] =
        Ref.make(baseDelay.toMillis).map { stateRef =>
          new EffectfulRetryPolicy.Attempt[Task] {
            def nextDelay(attempt: Int): Task[Option[FiniteDuration]] =
              stateRef.modify { previousDelayMs =>
                val (next, delay) =
                  RetryPolicy.decorrelatedJitterStep(baseDelay.toMillis, maxDelay.toMillis, maxRetries)(
                    previousDelayMs,
                    attempt
                  )
                (delay, next)
              }
          }
        }
    }
}
