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

sealed trait RetryPolicy {
  def nextDelay(attempt: Int): Option[FiniteDuration]
}

object RetryPolicy {

  case object NoRetry extends RetryPolicy {
    def nextDelay(attempt: Int): Option[FiniteDuration] = None
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
    def nextDelay(attempt: Int): Option[FiniteDuration] =
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
