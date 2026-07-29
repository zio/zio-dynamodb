package zio.dynamodb

import zio.test._

import scala.concurrent.duration._

object RetryPolicySpec extends ZIOSpecDefault {

  def spec = suite("RetryPolicy")(
    suite("NoRetry")(
      test("nextDelay always returns None") {
        assertTrue(
          RetryPolicy.NoRetry.nextDelay(0).isEmpty &&
            RetryPolicy.NoRetry.nextDelay(1).isEmpty &&
            RetryPolicy.NoRetry.nextDelay(99).isEmpty
        )
      }
    ),

    suite("ExponentialBackoff — jitter = false (deterministic)")(
      test("returns None when attempt >= maxRetries") {
        val p = RetryPolicy.ExponentialBackoff(maxRetries = 3, 100.millis, jitter = false)
        assertTrue(
          p.nextDelay(3).isEmpty &&
            p.nextDelay(4).isEmpty
        )
      },
      test("delay for attempt 0 equals initialDelay") {
        val p = RetryPolicy.ExponentialBackoff(maxRetries = 3, 100.millis, jitter = false)
        assertTrue(p.nextDelay(0).contains(100.millis))
      },
      test("delay doubles on each attempt (factor = 2.0)") {
        val p = RetryPolicy.ExponentialBackoff(maxRetries = 5, 100.millis, jitter = false)
        assertTrue(
          p.nextDelay(0).contains(100.millis) &&
            p.nextDelay(1).contains(200.millis) &&
            p.nextDelay(2).contains(400.millis)
        )
      },
      test("delay is capped at maxDelay") {
        val p = RetryPolicy.ExponentialBackoff(
          maxRetries = 10,
          initialDelay = 100.millis,
          maxDelay = 250.millis,
          jitter = false
        )
        assertTrue(
          p.nextDelay(2).contains(250.millis) &&
            p.nextDelay(5).contains(250.millis)
        )
      },
      test("custom factor is applied") {
        val p = RetryPolicy.ExponentialBackoff(
          maxRetries = 5,
          initialDelay = 100.millis,
          factor = 3.0,
          jitter = false
        )
        assertTrue(
          p.nextDelay(0).contains(100.millis) &&
            p.nextDelay(1).contains(300.millis) &&
            p.nextDelay(2).contains(900.millis)
        )
      }
    ),

    suite("ExponentialBackoff — jitter = true")(
      test("delay is in [0, cap] for each attempt") {
        val p       = RetryPolicy.ExponentialBackoff(maxRetries = 5, 100.millis, jitter = true)
        val results = (0 until 4).map(p.nextDelay(_).get.toMillis)
        val caps    = Seq(100L, 200L, 400L, 800L)
        assertTrue(results.zip(caps).forall { case (d, cap) => d >= 0L && d <= cap })
      },
      test("returns None after maxRetries with jitter enabled") {
        val p = RetryPolicy.ExponentialBackoff(maxRetries = 2, 50.millis, jitter = true)
        assertTrue(p.nextDelay(2).isEmpty)
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
