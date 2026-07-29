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
 * Batch execution with built-in retry.
 *
 *  Each method executes exactly one batch (≤ 25 items for write, ≤ 100
 *  for get) and retries the unprocessed residual according to `policy`.
 *  Splitting a larger collection into correctly-sized batches and
 *  constructing the query are the caller's responsibility — use
 *  [[DynamoDBQuery.batchWriteItem]] and [[DynamoDBQuery.batchGetItem]]
 *  to build the query before passing it here.
 *
 *  `policy` governs two independent retry loops:
 *  - Effect-level: transient failures (throttling, network) are retried
 *    via [[AwsInterpreter.withRetry]] on each individual attempt.
 *  - Response-level: unprocessed items are re-submitted until either all
 *    items are processed or the policy is exhausted.
 *
 *  Non-fatal effect errors (e.g. throttling, SDK validation failures) are
 *  captured in [[Batch.WriteResult.Failed]] / [[Batch.GetResult.Failed]]
 *  rather than raising a failed effect.  Fatal errors (e.g.
 *  `OutOfMemoryError`, which the effect runtime treats as a defect) bypass
 *  this wrapping and propagate as a failed `F` — the caller must not attempt
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

  /**
   * Executes a single [[DynamoDBQuery.BatchWriteItem]] with retry.
   *
   *  The retry policy is read from `q.retryPolicy`. Attach one with
   *  `q.withRetryPolicy(policy)` before calling this method. If no policy is
   *  set, the batch runs once with no retries.
   *
   *  @param interpreter the effect interpreter to run the query through
   *  @param q           the batch query — build with [[DynamoDBQuery.batchWriteItem]]
   *  @return [[Batch.WriteResult.Complete]] if all items were processed,
   *          [[Batch.WriteResult.Incomplete]] if unprocessed items remain after policy
   *          exhaustion, or [[Batch.WriteResult.Failed]] if an effect-level error persisted
   *          through all retries (exception captured in `cause`)
   */
  def runWriteItem[F[_]](
    interpreter: AwsInterpreter[F]
  )(q: DynamoDBQuery[Any, DynamoDBQuery.BatchWriteItem.Response]): F[Batch.WriteResult] =
    q match {
      case bw: DynamoDBQuery.BatchWriteItem =>
        retryWrite(interpreter, bw.retryPolicy.getOrElse(RetryPolicy.NoRetry), attempt = 0, bw)
      case other                            =>
        interpreter.pure(
          WriteResult.Failed(
            new IllegalArgumentException(s"Expected BatchWriteItem, got ${other.getClass.getSimpleName}"),
            0,
            0
          )
        )
    }

  /**
   * Executes a single [[DynamoDBQuery.BatchGetItem]] with retry.
   *
   *  The retry policy is read from `q.retryPolicy`. Attach one with
   *  `q.withRetryPolicy(policy)` before calling this method. If no policy is
   *  set, the batch runs once with no retries.
   *
   *  @param interpreter the effect interpreter to run the query through
   *  @param q           the batch query — build with [[DynamoDBQuery.batchGetItem]]
   *  @return [[Batch.GetResult.Complete]] if all keys were retrieved,
   *          [[Batch.GetResult.Incomplete]] if unprocessed keys remain after policy
   *          exhaustion, or [[Batch.GetResult.Failed]] if an effect-level error persisted
   *          through all retries (exception captured in `cause`)
   */
  def runGetItem[F[_]](
    interpreter: AwsInterpreter[F]
  )(q: DynamoDBQuery[Any, DynamoDBQuery.BatchGetItem.Response]): F[Batch.GetResult] =
    q match {
      case bg: DynamoDBQuery.BatchGetItem =>
        retryGet(interpreter, bg.retryPolicy.getOrElse(RetryPolicy.NoRetry), attempt = 0, bg)
      case other                          =>
        interpreter.pure(
          GetResult.Failed(
            new IllegalArgumentException(s"Expected BatchGetItem, got ${other.getClass.getSimpleName}"),
            0,
            0
          )
        )
    }

  private def retryWrite[F[_]](
    interp: AwsInterpreter[F],
    policy: RetryPolicy,
    attempt: Int,
    q: DynamoDBQuery.BatchWriteItem
  ): F[Batch.WriteResult] =
    interp.flatMap(interp.withRetryTracked(policy)(interp.run(q.copy(retryPolicy = None)))) {
      case Left((cause, effectRetries)) =>
        interp.pure(WriteResult.Failed(cause, responseRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        response.unprocessedItems match {
          case None            => interp.pure(WriteResult.Complete(response))
          case Some(remaining) =>
            policy.nextDelay(attempt) match {
              case None    => interp.pure(WriteResult.Incomplete(response))
              case Some(d) =>
                interp.flatMap(interp.sleep(d)) { _ =>
                  retryWrite(
                    interp,
                    policy,
                    attempt + 1,
                    DynamoDBQuery.BatchWriteItem(requestItems = remaining)
                  )
                }
            }
        }
    }

  private def retryGet[F[_]](
    interp: AwsInterpreter[F],
    policy: RetryPolicy,
    attempt: Int,
    q: DynamoDBQuery.BatchGetItem
  ): F[Batch.GetResult] =
    interp.flatMap(interp.withRetryTracked(policy)(interp.run(q.copy(retryPolicy = None)))) {
      case Left((cause, effectRetries)) =>
        interp.pure(GetResult.Failed(cause, responseRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        if (response.unprocessedKeys.isEmpty)
          interp.pure(GetResult.Complete(response))
        else
          policy.nextDelay(attempt) match {
            case None    => interp.pure(GetResult.Incomplete(response))
            case Some(d) =>
              interp.flatMap(interp.sleep(d)) { _ =>
                retryGet(
                  interp,
                  policy,
                  attempt + 1,
                  DynamoDBQuery.BatchGetItem(requestItems = response.unprocessedKeys)
                )
              }
          }
    }
}
