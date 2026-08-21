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
 * Result types for batch execution with built-in retry.
 *
 *  Batch queries run through the same [[AwsInterpreter.run]] entry point as every other
 *  [[DynamoDBQuery]] — build one with [[DynamoDBQuery.batchWriteItem]] or
 *  [[DynamoDBQuery.batchGetItem]], attach a [[RetryPolicy]] with `.withRetryPolicy(policy)`
 *  if desired, and call `interpreter.run(query)`. The interpreter honors the AWS batch retry
 *  contract internally — resubmitting unprocessed keys/items — so the caller never has to
 *  implement that loop themselves.
 *
 *  `policy` (read from the query's own `retryPolicy` field) governs two independent retry
 *  loops, both driven inside the interpreter's internal request-dispatch loop:
 *  - Effect-level: transient failures (throttling, network) are retried on each individual
 *    attempt.
 *  - Response-level: unprocessed items are re-submitted until either all items are
 *    processed or the policy is exhausted. If no policy is attached, the batch runs once
 *    with no retries ([[RetryPolicy.NoRetry]]).
 *
 *  Non-fatal effect errors (e.g. throttling, SDK validation failures) are captured in
 *  [[Batch.WriteResult.Failed]] / [[Batch.GetResult.Failed]] rather than raising a failed
 *  effect. Fatal errors (e.g. `OutOfMemoryError`, which the effect runtime treats as a
 *  defect) bypass this wrapping and propagate as a failed `F` — the caller must not attempt
 *  to recover from them as ordinary batch failures.
 */
object Batch {

  sealed trait WriteResult
  object WriteResult {

    /** All items processed — response has no unprocessed items. */
    final case class Complete(response: DynamoDBQuery.BatchWriteItem.Response) extends WriteResult

    /**
     * Policy exhausted with items still unprocessed (response-level). The AWS calls
     *  succeeded; DynamoDB kept returning leftovers until retries ran out.
     */
    final case class Incomplete(response: DynamoDBQuery.BatchWriteItem.Response) extends WriteResult

    /**
     * Effect-level failure (throttling, network) after all retries exhausted.
     *  `responseRetries` is the number of response-level resubmissions before the failure
     *  (0 = failed on the first submission).
     *  `effectRetries` is the number of effect-level retries within that submission
     *  (0 = failed on the first attempt with no retries).
     */
    final case class Failed(cause: Throwable, responseRetries: Int, effectRetries: Int) extends WriteResult
  }

  sealed trait GetResult
  object GetResult {

    /** All keys retrieved — response has no unprocessed keys. */
    final case class Complete(response: DynamoDBQuery.BatchGetItem.Response) extends GetResult

    /** Policy exhausted with keys still unprocessed (response-level). */
    final case class Incomplete(response: DynamoDBQuery.BatchGetItem.Response) extends GetResult

    /**
     * Effect-level failure after all retries exhausted.
     *  `responseRetries` is the number of response-level resubmissions before the failure
     *  (0 = failed on the first submission).
     *  `effectRetries` is the number of effect-level retries within that submission
     *  (0 = failed on the first attempt with no retries).
     */
    final case class Failed(cause: Throwable, responseRetries: Int, effectRetries: Int) extends GetResult
  }
}
