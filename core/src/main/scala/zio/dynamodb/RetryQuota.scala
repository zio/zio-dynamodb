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

/**
 * A client-scoped circuit breaker gating whether a retry should happen at all, independent of
 * any one call's own backoff curve — parity with the AWS SDK's own token-bucket retry quota.
 * Unlike [[RetryPolicy]]/[[EffectfulRetryPolicy]], which are deliberately isolated per
 * execution, a `RetryQuota` is deliberately **shared**: one instance, one budget, mutated by
 * every concurrent call through the same interpreter, because the signal it tracks — "has this
 * client been failing a lot lately" — is only meaningful in aggregate.
 *
 * Held once as an `AwsInterpreter[F]` member, constructed once per interpreter. Mirrors AWS's
 * own model: each retry attempt debits a cost (see `RetryPolicy.retryCost`/`isThrottlingException`
 * for the 14-transient/5-throttling split); a request that succeeds without retrying credits 1
 * token back, and one that succeeds after retrying credits back exactly what its own retries
 * cost. Exhaustion fails fast the same way curve-exhaustion does — no retry, no `onRetry` fired.
 */
trait RetryQuota[F[_]] {

  /** True and debited if there's enough budget to retry; false (no debit) if exhausted. */
  def tryConsume(cost: Int): F[Boolean]

  /** Restores tokens on eventual success — `amount` is what this execution's own retries cost. */
  def credit(amount: Int): F[Unit]
}
