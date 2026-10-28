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

import scala.concurrent.duration.FiniteDuration

/**
 * The interpreter-level counterpart to [[RetryPolicy]], given as a `fromAsyncClient` factory's
 * `defaultRetryPolicy` for queries that don't set `.withRetryPolicy`. A `RetryPolicy` attaches to a
 * query before an effect type is chosen; this one knows `F`, so it can use the effect system's own
 * state and retry types (e.g. a ZIO `Ref` or `Schedule`).
 */
trait EffectfulRetryPolicy[F[_]] {
  def newAttempt(): F[EffectfulRetryPolicy.Attempt[F]]
}

object EffectfulRetryPolicy {

  /** Per-execution retry state — the effectful counterpart to [[RetryPolicy.Attempt]]. */
  trait Attempt[F[_]] {
    def nextDelay(attempt: Int): F[Option[FiniteDuration]]
  }
}
