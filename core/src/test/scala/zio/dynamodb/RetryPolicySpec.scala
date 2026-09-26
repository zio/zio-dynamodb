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

import zio.test._

import scala.concurrent.duration._

object RetryPolicySpec extends ZIOSpecDefault {

  def spec = suite("RetryPolicy")(
    suite("NoRetry")(
      test("nextDelay always returns None") {
        val attempt = RetryPolicy.NoRetry.newAttempt()
        assertTrue(
          attempt.nextDelay(0).isEmpty &&
            attempt.nextDelay(1).isEmpty &&
            attempt.nextDelay(99).isEmpty
        )
      }
    ),

    suite("ExponentialBackoff — jitter = false (deterministic)")(
      test("returns None when attempt >= maxRetries") {
        val attempt = RetryPolicy.ExponentialBackoff(maxRetries = 3, 100.millis, jitter = false).newAttempt()
        assertTrue(
          attempt.nextDelay(3).isEmpty &&
            attempt.nextDelay(4).isEmpty
        )
      },
      test("delay for attempt 0 equals initialDelay") {
        val attempt = RetryPolicy.ExponentialBackoff(maxRetries = 3, 100.millis, jitter = false).newAttempt()
        assertTrue(attempt.nextDelay(0).contains(100.millis))
      },
      test("delay doubles on each attempt (factor = 2.0)") {
        val attempt = RetryPolicy.ExponentialBackoff(maxRetries = 5, 100.millis, jitter = false).newAttempt()
        assertTrue(
          attempt.nextDelay(0).contains(100.millis) &&
            attempt.nextDelay(1).contains(200.millis) &&
            attempt.nextDelay(2).contains(400.millis)
        )
      },
      test("delay is capped at maxDelay") {
        val attempt = RetryPolicy
          .ExponentialBackoff(
            maxRetries = 10,
            initialDelay = 100.millis,
            maxDelay = 250.millis,
            jitter = false
          )
          .newAttempt()
        assertTrue(
          attempt.nextDelay(2).contains(250.millis) &&
            attempt.nextDelay(5).contains(250.millis)
        )
      },
      test("custom factor is applied") {
        val attempt = RetryPolicy
          .ExponentialBackoff(
            maxRetries = 5,
            initialDelay = 100.millis,
            factor = 3.0,
            jitter = false
          )
          .newAttempt()
        assertTrue(
          attempt.nextDelay(0).contains(100.millis) &&
            attempt.nextDelay(1).contains(300.millis) &&
            attempt.nextDelay(2).contains(900.millis)
        )
      }
    ),

    suite("ExponentialBackoff — jitter = true")(
      test("delay is in [0, cap] for each attempt") {
        val attempt = RetryPolicy.ExponentialBackoff(maxRetries = 5, 100.millis, jitter = true).newAttempt()
        val results = (0 until 4).map(attempt.nextDelay(_).get.toMillis)
        val caps    = Seq(100L, 200L, 400L, 800L)
        assertTrue(results.zip(caps).forall { case (d, cap) => d >= 0L && d <= cap })
      },
      test("returns None after maxRetries with jitter enabled") {
        val attempt = RetryPolicy.ExponentialBackoff(maxRetries = 2, 50.millis, jitter = true).newAttempt()
        assertTrue(attempt.nextDelay(2).isEmpty)
      }
    ),

    suite("custom")(
      test("stateless curve behaves per the supplied function") {
        val policy  = RetryPolicy.custom(attempt =>
          if (attempt >= 3) None else Some(FiniteDuration(attempt * 100L, "milliseconds"))
        )
        val attempt = policy.newAttempt()
        assertTrue(
          attempt.nextDelay(0).contains(0.millis) &&
            attempt.nextDelay(1).contains(100.millis) &&
            attempt.nextDelay(2).contains(200.millis) &&
            attempt.nextDelay(3).isEmpty
        )
      }
    ),

    suite("statefulCustom")(
      test("state is scoped per newAttempt() call, not shared across attempts of the same instance") {
        val policy = RetryPolicy.statefulCustom { () =>
          var count = 0
          (_: Int) => {
            count += 1
            Some(FiniteDuration(count.toLong, "milliseconds"))
          }
        }
        val first  = policy.newAttempt()
        val second = policy.newAttempt()
        assertTrue(
          first.nextDelay(0).contains(1.millis) &&
            first.nextDelay(0).contains(2.millis) &&
            second.nextDelay(0).contains(1.millis) // fresh state — not 3.millis
        )
      }
    ),

    suite("isRetryable")(
      test("matches ProvisionedThroughputExceededException") {
        val t = new RuntimeException("ProvisionedThroughputExceededException")
        assertTrue(RetryPolicy.isRetryable(t))
      },
      test("matches ThrottlingException") {
        val t = new RuntimeException("ThrottlingException: rate exceeded")
        assertTrue(RetryPolicy.isRetryable(t))
      },
      test("does not match unrelated errors") {
        val t = new RuntimeException("NullPointerException")
        assertTrue(!RetryPolicy.isRetryable(t))
      },
      test("does not match null message") {
        val t = new RuntimeException()
        assertTrue(!RetryPolicy.isRetryable(t))
      }
    )
  )
}
