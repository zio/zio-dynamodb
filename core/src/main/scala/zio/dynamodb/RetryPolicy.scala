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

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.FiniteDuration

/**
 * Governs retry timing for a query, attached via `.withRetryPolicy(policy)`. A policy is
 * reusable and shareable across many query executions; `newAttempt()` creates fresh,
 * execution-scoped state for one retry sequence, so a stateful curve (e.g. decorrelated
 * jitter, built via [[RetryPolicy.statefulCustom]]) never leaks state across concurrent,
 * unrelated executions sharing the same policy value.
 */
sealed trait RetryPolicy {
  def newAttempt(): RetryPolicy.Attempt
}

/**
 * [[RetryPolicy.NoRetry]] (the default), [[RetryPolicy.ExponentialBackoff]],
 * [[RetryPolicy.custom]]/[[RetryPolicy.statefulCustom]], and the default
 * [[RetryPolicy.isRetryable]] predicate.
 */
object RetryPolicy {

  /**
   * Per-execution retry state. `nextDelay` is consulted once per failed attempt of one retry
   * sequence — `None` means stop retrying and raise the error; `Some(d)` means wait `d` then
   * try again. Driven internally by `AwsInterpreter.withRetry` for effect-level failures, and
   * by the batch-specific retry loop in `Batch` for response-level unprocessed items/keys.
   */
  trait Attempt {
    def nextDelay(attempt: Int): Option[FiniteDuration]
  }

  case object NoRetry extends RetryPolicy {
    private val noRetryAttempt: Attempt = (_: Int) => None
    def newAttempt(): Attempt           = noRetryAttempt
  }

  /**
   * Full-jitter exponential backoff.
   *
   * cap(n)   = min(initialDelay * factor^n, maxDelay)
   * delay(n) = random[0, cap(n)]   when jitter = true (default)
   *          = cap(n)               when jitter = false
   *
   * `jitter = false` is provided for deterministic tests only.
   */
  final case class ExponentialBackoff(
    maxRetries: Int,
    initialDelay: FiniteDuration,
    factor: Double = 2.0,
    maxDelay: FiniteDuration = FiniteDuration(30, "seconds"),
    jitter: Boolean = true
  ) extends RetryPolicy {
    def newAttempt(): Attempt = { (attempt: Int) =>
      if (attempt >= maxRetries) None
      else {
        val cap = math
          .min(
            initialDelay.toMillis * math.pow(factor, attempt.toDouble),
            maxDelay.toMillis
          )
          .toLong
        val ms  =
          if (jitter) ThreadLocalRandom.current().nextLong(0L, cap + 1L)
          else cap
        Some(FiniteDuration(ms, "milliseconds"))
      }
    }
  }

  private[dynamodb] final case class Custom(newState: () => Attempt) extends RetryPolicy {
    def newAttempt(): Attempt = newState()
  }

  /** Stateless curve — most cases (linear, constant, custom caps). */
  def custom(f: Int => Option[FiniteDuration]): RetryPolicy =
    Custom(() => (attempt: Int) => f(attempt))

  /**
   * Stateful curve (e.g. decorrelated jitter — AWS's own recommended algorithm,
   * `sleep = min(cap, random_between(base, previous_sleep * 3))`). `newAttempt` runs once per
   * retry sequence; anything it captures (a plain `var`, no atomics needed) is genuinely
   * scoped to that one execution, since nothing else can reach it.
   *
   * {{{
   * RetryPolicy.statefulCustom { () =>
   *   var previousDelay = 100L
   *   (attempt: Int) =>
   *     if (attempt >= 8) None
   *     else {
   *       val next = math.min(20000L, 100L + ThreadLocalRandom.current().nextLong(0, previousDelay * 3 - 100L + 1))
   *       previousDelay = next
   *       Some(FiniteDuration(next, "milliseconds"))
   *     }
   * }
   * }}}
   */
  def statefulCustom(newAttempt: () => Int => Option[FiniteDuration]): RetryPolicy =
    Custom { () =>
      val f: Int => Option[FiniteDuration] = newAttempt()
      (attempt: Int) => f(attempt)
    }

  /** Default predicate covering standard DynamoDB transient errors. */
  val isRetryable: Throwable => Boolean = { t =>
    val msg = t.getMessage
    msg != null && (
      msg.contains("ProvisionedThroughputExceededException") ||
        msg.contains("RequestLimitExceeded") ||
        msg.contains("ServiceUnavailable") ||
        msg.contains("ThrottlingException")
    )
  }
}
