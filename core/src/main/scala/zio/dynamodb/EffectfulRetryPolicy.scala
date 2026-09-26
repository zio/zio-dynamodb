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
 * An effectful, interpreter-level counterpart to [[RetryPolicy]] — supplies the fallback used
 * when a query doesn't specify its own `.withRetryPolicy(...)` (`AwsInterpreter.defaultRetryPolicy`).
 * `RetryPolicy` stays effect-neutral because it attaches to a `DynamoDBQuery` before any
 * interpreter/effect type is chosen; `EffectfulRetryPolicy` only ever exists at the interpreter
 * level, where a concrete `F` is already known, so it can model state via that effect system's
 * own primitives (e.g. a ZIO `Ref`) and wrap richer per-effect-system retry abstractions (e.g.
 * ZIO's `Schedule`).
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
