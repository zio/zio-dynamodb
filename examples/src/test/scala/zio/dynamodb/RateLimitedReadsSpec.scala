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
import zio.test._
import zio.test.TestClock

object RateLimitedReadsSpec extends ZIOSpecDefault {

  private def getResponse(rcus: Double): DynamoDBResponseMetadata =
    DynamoDBResponseMetadata.GetItem(
      tableName = "orders",
      consumed = Some(ConsumedCapacity(None, Some(rcus), None, Map.empty, Map.empty)),
      correlation = CorrelationContext(None)
    )

  private def putResponse: DynamoDBResponseMetadata =
    DynamoDBResponseMetadata.PutItem(
      tableName = "orders",
      consumed = None,
      collectionMetrics = None,
      correlation = CorrelationContext(None)
    )

  def spec = suite("RateLimitedReadsSpec")(
    test("consuming less than the available budget doesn't sleep") {
      for {
        interceptor <- RateLimitedReads.rcuRateLimiter(rcusPerSecond = 10.0)
        start       <- Clock.nanoTime
        _           <- interceptor.onResponse(getResponse(5.0))
        end         <- Clock.nanoTime
      } yield assertTrue(end == start)
    },
    test("consuming more than the available budget sleeps for exactly the deficit") {
      for {
        interceptor <- RateLimitedReads.rcuRateLimiter(rcusPerSecond = 10.0)
        // bucket starts full (10 tokens); 20 RCU leaves a 10-unit deficit == 1s at 10 RCU/s
        fiber       <- interceptor.onResponse(getResponse(20.0)).fork
        _           <- TestClock.adjust(999.millis)
        notYetDone  <- fiber.poll.map(_.isEmpty)
        _           <- TestClock.adjust(1.milli)
        _           <- fiber.join
      } yield assertTrue(notYetDone)
    },
    test("a write response (0 RCU) never sleeps") {
      for {
        interceptor <- RateLimitedReads.rcuRateLimiter(rcusPerSecond = 10.0)
        start       <- Clock.nanoTime
        _           <- interceptor.onResponse(putResponse)
        end         <- Clock.nanoTime
      } yield assertTrue(end == start)
    },
    test("rejects non-positive, NaN, or infinite rates") {
      // A zero rate would divide by zero on the deficit calculation, turning ordinary
      // consumption into an effectively unbounded (or, once a pending reservation is
      // added on top, overflowing) sleep; negative/NaN/infinite rates break the token
      // bucket's arithmetic in their own ways. Reject all of them up front.
      def rejects(rate: Double) =
        ZIO.attempt(RateLimitedReads.rcuRateLimiter(rate)).exit.map(_.isFailure)

      for {
        zero     <- rejects(0.0)
        negative <- rejects(-1.0)
        nan      <- rejects(Double.NaN)
        infinite <- rejects(Double.PositiveInfinity)
      } yield assertTrue(zero, negative, nan, infinite)
    },
    test(
      "concurrent over-budget responses stack their reservations instead of overwriting " +
        "each other's pending deadline"
    ) {
      // Regression test for a Copilot-flagged race: each response's `now` is a private
      // snapshot taken before its Ref.modify runs, so a second racing response computing
      // its wait without accounting for a first response's already-pending future
      // reservation would silently shorten (or erase) that reservation — letting both
      // responses become visible sooner than the configured rate allows.
      //
      // Primed to a known-empty bucket first so the result doesn't depend on which of the
      // two racing responses' Ref.modify happens to run first — whichever runs first gets
      // to spend part of the bucket's ongoing refill and finishes sooner on its own (that's
      // expected and fine); the invariant that must hold regardless of order is that the
      // *later*-processed one always pays for the earlier one's still-pending reservation
      // on top of its own marginal cost, so the group as a whole never finishes before
      // (20 + 10) RCU / 10 RCU-per-sec = 3s. The bug this guards against always finishes
      // the group in 2s instead (the larger of the two responses' own, un-stacked,
      // individual waits) — 1s early, in either interleaving.
      for {
        interceptor <- RateLimitedReads.rcuRateLimiter(rcusPerSecond = 10.0)
        _           <- interceptor.onResponse(getResponse(10.0)) // drains the initial free burst, no wait
        fiberA      <- interceptor.onResponse(getResponse(20.0)).fork
        fiberB      <- interceptor.onResponse(getResponse(10.0)).fork
        _           <- TestClock.adjust(2999.millis)
        bothDone    <- fiberA.poll.zip(fiberB.poll).map { case (a, b) => a.isDefined && b.isDefined }
        _           <- TestClock.adjust(1.milli)
        _           <- fiberA.join
        _           <- fiberB.join
      } yield assertTrue(!bothDone)
    }
  )
}
