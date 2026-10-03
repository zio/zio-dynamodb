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

import cats.effect.{ IO, Ref }

/** Cats-Effect-specific [[RetryQuota]] constructor. */
object CERetryQuota {

  private final class RefBacked(ref: Ref[IO, Int], capacity: Int) extends RetryQuota[IO] {
    def tryConsume(cost: Int): IO[Boolean] =
      ref.modify(balance => if (balance >= cost) (balance - cost, true) else (balance, false))

    def credit(amount: Int): IO[Unit] =
      ref.update(balance => math.min(capacity, balance + amount))
  }

  /**
   * AWS's own token-bucket shape: starts at `capacity` (500, matching AWS's own default),
   * debited per retry attempt via `tryConsume`, credited back on success via `credit`, capped
   * at `capacity` so a long healthy streak can't accumulate more than the starting balance.
   * Constructed synchronously (not `IO[RetryQuota[IO]]`) so it fits `fromAsyncClient`'s plain
   * default-parameter shape, same as `CatsRetryPolicies.fullJitter()`.
   */
  def standard(capacity: Int = 500): RetryQuota[IO] = {
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    new RefBacked(Ref.unsafe[IO, Int](capacity), capacity)
  }
}
