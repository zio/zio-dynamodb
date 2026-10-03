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
 * [[RetryPolicy.custom]]/[[RetryPolicy.statefulCustom]], [[RetryPolicy.fullJitter]], and the
 * default [[RetryPolicy.isRetryable]] predicate.
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
    // Stateless — every attempt is a pure function of `attempt` and this case class's own
    // immutable fields (ThreadLocalRandom is itself thread-local), so one Attempt instance is
    // safe to share across every newAttempt() call and every concurrent execution, same as
    // NoRetry's cached singleton above.
    private val cachedAttempt: Attempt = { (attempt: Int) =>
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
    def newAttempt(): Attempt          = cachedAttempt
  }

  private[dynamodb] final case class Custom(newState: () => Attempt) extends RetryPolicy {
    def newAttempt(): Attempt = newState()
  }

  /** Stateless curve — most cases (linear, constant, custom caps). */
  def custom(f: Int => Option[FiniteDuration]): RetryPolicy =
    Custom(() => (attempt: Int) => f(attempt))

  /**
   * Stateful curve (e.g. decorrelated jitter — `sleep = min(cap, random_between(base,
   * previous_sleep * 3))`). `newAttempt` runs once per retry sequence; anything it captures (a
   * plain `var`, no atomics needed) is genuinely scoped to that one execution, since nothing
   * else can reach it.
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

  /**
   * Full-jitter exponential backoff: `delay = random(0, min(maxDelay, baseDelay * 2^attempt))`
   * — a thin preset over [[ExponentialBackoff]] fixing `factor = 2.0`/`jitter = true`. This is
   * what AWS SDKs actually implement as their current standard retry mode (see AWS's SDKs and
   * Tools Reference Guide, "Retry behavior").
   */
  def fullJitter(
    maxRetries: Int = 8,
    baseDelay: FiniteDuration = FiniteDuration(100, "milliseconds"),
    maxDelay: FiniteDuration = FiniteDuration(20, "seconds")
  ): RetryPolicy =
    ExponentialBackoff(maxRetries, baseDelay, factor = 2.0, maxDelay, jitter = true)

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

  /**
   * Token cost for [[RetryQuota]], consulted only for an error `isRetryable` already matched —
   * mirrors AWS's own two-tier split (5 for throttling, 14 for other transient errors;
   * see AWS SDKs and Tools Reference Guide, "Retry quota management"). Message-substring based,
   * like `isRetryable`; `RealAwsInterpreter`'s override in `aws` classifies from the SDK's own
   * `AwsServiceException#isThrottlingException` instead.
   */
  val retryCost: Throwable => Int = { t =>
    val msg          = t.getMessage
    val isThrottling = msg != null && (
      msg.contains("ProvisionedThroughputExceededException") ||
        msg.contains("RequestLimitExceeded") ||
        msg.contains("ThrottlingException")
    )
    if (isThrottling) 5 else 14
  }
}
