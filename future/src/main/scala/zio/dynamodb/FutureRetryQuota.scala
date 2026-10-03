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

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

/**
 * Future-specific [[RetryQuota]] constructor. Unlike `FutureRetryPolicies`' per-execution
 * `var` (safe because one execution's own calls never overlap), a quota's balance is genuinely
 * shared and mutated by every concurrent call through the same interpreter — `Future` has no
 * `Ref`/STM-equivalent for that, so this uses a plain `AtomicInteger` with a compare-and-swap
 * retry loop instead.
 */
object FutureRetryQuota {

  private final class AtomicBacked(capacity: Int) extends RetryQuota[Future] {
    private val balance = new AtomicInteger(capacity)

    def tryConsume(cost: Int): Future[Boolean] = {
      def loop(): Boolean = {
        val current = balance.get()
        if (current < cost) false
        else if (balance.compareAndSet(current, current - cost)) true
        else loop()
      }
      Future.successful(loop())
    }

    def credit(amount: Int): Future[Unit] = {
      def loop(): Unit = {
        val current = balance.get()
        val next    = math.min(capacity, current + amount)
        if (!balance.compareAndSet(current, next)) loop()
      }
      Future.successful(loop())
    }
  }

  /**
   * AWS's own token-bucket shape: starts at `capacity` (500, matching AWS's own default),
   * debited per retry attempt via `tryConsume`, credited back on success via `credit`, capped
   * at `capacity` so a long healthy streak can't accumulate more than the starting balance.
   */
  def standard(capacity: Int = 500): RetryQuota[Future] = new AtomicBacked(capacity)
}
