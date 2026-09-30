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

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import zio.blocks.chunk.Chunk
import zio.dynamodb.DynamoDBError.{ ItemError, ScanError, TransactionError }

// -- Base trait ----------------------------------------------------------

/** The single entry point for running a [[DynamoDBQuery]]: `interpreter.run(query)`. */
trait Interpreter[F[_]] {
  def run[Out](query: DynamoDBQuery[_, Out]): F[Out]
}

// -- Abstract interpreter ------------------------------------------------
// Separates two concerns:
//   1. Effect primitives (pure, map, product, fail, absolve) — vary per F[_]
//   2. Per-operation methods (runGetItem, etc.)             — vary per backend
//
// ADT traversal (runAny) is shared here — written once for all interpreters.
// Lives in core with no AWS SDK dependency; DummyIOInterpreter compiles without it.

/**
 * Base class implementing [[Interpreter]]'s query traversal once for every backend.
 * Subclasses (`ZioInterpreter`, `CEInterpreter`, `FutureInterpreter`, each backed by the
 * `aws` module's `AwsDynamoDB`) supply two things: the effect primitives above
 * (`pure`/`map`/`flatMap`/`product`/`fail`/`absolve`/`sleep`/`attempt`/`raiseError`, one set
 * per effect type `F[_]`) and the per-operation `runXxx` methods that issue the actual AWS
 * SDK calls. Everything else — walking the query, applying [[RetryPolicy]] via
 * [[withRetry]], validating conditions — is written once here and shared by every backend.
 */
abstract class AwsInterpreter[F[_]] extends Interpreter[F] {

  // Effect primitives — supplied by each concrete interpreter.
  // flatMap and pure are widened to private[dynamodb] so BatchUtils
  // (same package, not a subclass) can drive the residual retry loop.
  private[dynamodb] def pure[A](a: A): F[A]
  private[dynamodb] def map[A, B](fa: F[A])(f: A => B): F[B]
  private[dynamodb] def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
  protected def product[A, B](fa: F[A], fb: F[B]): F[(A, B)]
  protected def productPar[A, B](fa: F[A], fb: F[B]): F[(A, B)]
  protected def fail[A](e: DynamoDBError): F[A]
  protected def absolve[A](fa: F[Either[ItemError, A]]): F[A]

  // Retry primitives — also private[dynamodb] for the same reason.
  private[dynamodb] def sleep(d: FiniteDuration): F[Unit]
  private[dynamodb] def attempt[A](fa: F[A]): F[Either[Throwable, A]]
  private[dynamodb] def raiseError[A](t: Throwable): F[A]

  /**
   * The predicate `runAny` uses when a query's `RetryPolicy` doesn't specify one of its own.
   *  `core` has no AWS SDK dependency, so this defaults to [[RetryPolicy.isRetryable]]'s
   *  message-substring check; `RealAwsInterpreter` (in `aws`) overrides it with a check
   *  against the SDK's own exception types instead.
   */
  protected def isRetryable: Throwable => Boolean = RetryPolicy.isRetryable

  /**
   * The fallback used when a query doesn't specify its own `.withRetryPolicy(...)`. This
   *  trait's own default is `None` (no retry); concrete interpreters override it via a
   *  `fromAsyncClient` factory parameter rather than subclassing, so a caller can swap or
   *  disable it in one line without touching interpreter internals.
   */
  protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[F]] = None

  /**
   * Fires just before an effect-level retry sleeps and retries — see [[RetryInterceptor]]. This
   *  trait's own default is `None`; concrete interpreters override it the same way as
   *  `defaultRetryPolicy`, via a `fromAsyncClient` factory parameter.
   */
  protected def retryInterceptor: Option[RetryInterceptor[F]] = None

  /**
   * Fires just before batch's response-level (unprocessed-items) resubmission loop sleeps and
   *  resubmits — see [[BatchRetryInterceptor]]. Independent of `retryInterceptor`: the
   *  response-level loop resubmits from a successful response with no `Throwable` involved, a
   *  genuinely different mechanism from the effect-level retry loop `retryInterceptor` covers,
   *  so a caller who only wants effect-level retry visibility has nothing to no-op here.
   */
  protected def batchRetryInterceptor: Option[BatchRetryInterceptor[F]] = None

  /**
   * Retries `fa` according to `policy` whenever `isRetryable` matches the
   *  thrown error. Exhausted retries re-raise the last error.
   *
   *  `fa` is by-name so each retry re-evaluates the effect. `onRetry` fires once per retried
   *  attempt, right before the delay is slept; defaults to a no-op so every existing caller
   *  (this method is public) keeps compiling unchanged.
   */
  final def withRetry[A](
    policy: RetryPolicy,
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[A] =
    // newAttempt() is deferred into the flatMap continuation so a reused F[A] value (e.g. a
    // caller holding `val effect = interp.run(query)` and running it more than once) gets a
    // fresh Attempt on every execution, not one shared across all of them.
    flatMap(pure(())) { _ =>
      val attemptState       = policy.newAttempt()
      def loop(n: Int): F[A] =
        flatMap(attempt(fa)) {
          case Right(a)                  => pure(a)
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            attemptState.nextDelay(n) match {
              case None    => raiseError(t)
              case Some(d) => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1)))
            }
          case Left(t)                   => raiseError(t)
        }
      loop(0)
    }

  /**
   * Like [[withRetry]] but returns the number of effect-level retries made alongside
   *  the result. On success yields `Right(a)`. On exhaustion yields `Left((cause, retries))`
   *  where `retries` is the number of retries attempted (0 = failed on first call).
   */
  private[dynamodb] final def withRetryTracked[A](
    policy: RetryPolicy,
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] =
    // See withRetry — newAttempt() deferred so a reused F[A] value gets a fresh Attempt per run.
    flatMap(pure(())) { _ =>
      val attemptState                                 = policy.newAttempt()
      def loop(n: Int): F[Either[(Throwable, Int), A]] =
        flatMap(attempt(fa)) {
          case Right(a)                  => pure(Right(a))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            attemptState.nextDelay(n) match {
              case None    => pure(Left((t, n)))
              case Some(d) => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1)))
            }
          case Left(t)                   => pure(Left((t, n)))
        }
      loop(0)
    }

  /** Like [[withRetry]], but for an [[EffectfulRetryPolicy]] — see `defaultRetryPolicy`. */
  final def withRetryF[A](
    policy: EffectfulRetryPolicy[F],
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[A] =
    flatMap(policy.newAttempt()) { attemptState =>
      def loop(n: Int): F[A] =
        flatMap(attempt(fa)) {
          case Right(a)                  => pure(a)
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            flatMap(attemptState.nextDelay(n)) {
              case None    => raiseError(t)
              case Some(d) => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1)))
            }
          case Left(t)                   => raiseError(t)
        }
      loop(0)
    }

  /**
   * The full per-query retry fallback chain: the query's own policy if it set one, else
   *  `defaultRetryPolicy` if the interpreter has one, else no retry at all (today's behavior).
   */
  private def withOptionalRetry[A](
    retryPolicy: Option[RetryPolicy],
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[A] =
    retryPolicy match {
      case Some(p) => withRetry(p, isRetryable, onRetry)(fa)
      case None    =>
        defaultRetryPolicy match {
          case Some(p) => withRetryF(p, isRetryable, onRetry)(fa)
          case None    => fa
        }
    }

  /**
   * Like [[withOptionalRetry]], but never falls back to `defaultRetryPolicy` — only a query's
   *  own explicit `.withRetryPolicy(...)` is honored. Used for `UpdateItem`: its `Action` DSL
   *  includes non-idempotent updates (`.add`/`.increment`/`.decrement`/`.appendList`/
   *  `.prependList`), where retrying an ambiguous-outcome failure (the original write may have
   *  already landed) can silently double-apply a delta. The interpreter has no way to tell an
   *  idempotent `.set` from a non-idempotent one, so it can't safely opt every `UpdateItem`
   *  into retrying by default the way it does for `GetItem`/`PutItem`/`DeleteItem`/batch.
   */
  private def withExplicitRetryOnly[A](
    retryPolicy: Option[RetryPolicy],
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[A] =
    retryPolicy match {
      case Some(p) => withRetry(p, isRetryable, onRetry)(fa)
      case None    => fa
    }

  /** Like [[withRetryTracked]], but for an [[EffectfulRetryPolicy]] — see `defaultRetryPolicy`. */
  private[dynamodb] final def withRetryTrackedF[A](
    policy: EffectfulRetryPolicy[F],
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] =
    flatMap(policy.newAttempt()) { attemptState =>
      def loop(n: Int): F[Either[(Throwable, Int), A]] =
        flatMap(attempt(fa)) {
          case Right(a)                  => pure(Right(a))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            flatMap(attemptState.nextDelay(n)) {
              case None    => pure(Left((t, n)))
              case Some(d) => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1)))
            }
          case Left(t)                   => pure(Left((t, n)))
        }
      loop(0)
    }

  /**
   * Batch ops' effect-level retry fallback chain — mirrors [[withOptionalRetry]], but batch
   *  always goes through some form of [[withRetryTracked]] (even `RetryPolicy.NoRetry`) rather
   *  than skipping the wrapper entirely, since batch's errors-as-values design needs the
   *  attempt count and the `Either` shape regardless of whether retry actually happens.
   */
  private def withOptionalRetryTracked[A](
    retryPolicy: Option[RetryPolicy],
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] =
    retryPolicy match {
      case Some(p) => withRetryTracked(p, isRetryable, onRetry)(fa)
      case None    =>
        defaultRetryPolicy match {
          case Some(p) => withRetryTrackedF(p, isRetryable, onRetry)(fa)
          case None    => withRetryTracked(RetryPolicy.NoRetry, isRetryable, onRetry)(fa)
        }
    }

  /**
   * The delay-decision function for batch's response-level (unprocessed-items) resubmission
   *  loop — resolved once per batch call from the same fallback chain as
   *  [[withOptionalRetryTracked]], then threaded through the loop's recursion so a stateful
   *  `defaultRetryPolicy` only calls `newAttempt()` once per batch call, not once per
   *  resubmission.
   */
  private def newResponseLevelDelayFn(retryPolicy: Option[RetryPolicy]): F[Int => F[Option[FiniteDuration]]] =
    retryPolicy match {
      case Some(p) =>
        // See withRetry — newAttempt() deferred so a reused F[...] value gets a fresh Attempt per run.
        flatMap(pure(())) { _ =>
          val attemptState = p.newAttempt()
          pure((n: Int) => pure(attemptState.nextDelay(n)))
        }
      case None    =>
        defaultRetryPolicy match {
          case Some(p) => map(p.newAttempt())(attemptState => (n: Int) => attemptState.nextDelay(n))
          case None    => pure((_: Int) => pure(None))
        }
    }

  // Per-operation methods — each concrete interpreter or stub implements these.
  // In the aws submodule, RealAwsInterpreter provides default impls via AwsCodecs + AwsDynamoDB.
  protected def runGetItem(q: DynamoDBQuery.GetItem): F[Option[Item]]
  protected def runPutItem(q: DynamoDBQuery.PutItem): F[Option[Item]]
  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): F[Option[Item]]
  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): F[Option[Item]]
  protected def runQuery(q: DynamoDBQuery.Query): F[Page[Item]]
  protected def runScan(q: DynamoDBQuery.Scan): F[Page[Item]]
  protected def runCreateTable(q: DynamoDBQuery.CreateTable): F[Unit]
  protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): F[Unit]
  protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): F[DynamoDBQuery.DescribeTableResponse]
  protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): F[DynamoDBQuery.BatchGetItem.Response]
  protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): F[DynamoDBQuery.BatchWriteItem.Response]
  protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): F[Chunk[Option[Item]]]
  protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): F[Unit]

  private def accumulateErrors(msgs: List[String]): Option[DynamoDBError] =
    msgs match {
      case Nil    => None
      case h :: t =>
        Some(
          t.foldLeft(DynamoDBError.ItemError.DecodingError.failure(h))(
            _ ++ DynamoDBError.ItemError.DecodingError.failure(_)
          )
        )
    }

  // Returns Some(accumulated DecodingError) if the expression tree contains any Failure nodes.
  private def validateCE(ce: Option[ConditionExpression[_]]): Option[DynamoDBError] =
    accumulateErrors(ConditionExpression.collectFailures(ce))

  private def validateKCE(kce: Option[KeyConditionExpr[_]]): Option[DynamoDBError] =
    kce match {
      case None                                =>
        Some(
          DynamoDBError.QueryBuilderError.MissingKeyCondition(
            "'query' requires a key condition — did you forget '.whereKey(...)'?"
          )
        )
      case Some(KeyConditionExpr.Failure(msg)) => Some(DynamoDBError.ItemError.DecodingError.failure(msg))
      case _                                   => None
    }

  private def validateAction(action: UpdateExpression.Action[_]): Option[DynamoDBError] =
    accumulateErrors(UpdateExpression.collectFailures(action))

  // Returns Some(error) if segment fields are invalid, None if valid.
  private def validateScanSegment(q: DynamoDBQuery.Scan): Option[ScanError.ScanValidationError] =
    if (q.totalSegments < 1)
      Some(
        ScanError.ScanValidationError(
          s"totalSegments must be >= 1; got ${q.totalSegments}"
        )
      )
    else if (q.segment < 0 || q.segment >= q.totalSegments)
      Some(
        ScanError.ScanValidationError(
          s"segment must be in [0, totalSegments); got segment=${q.segment}, totalSegments=${q.totalSegments}"
        )
      )
    else None

  // Returns Some(error) if count is outside [1, 100], None if valid.
  private def validateTransactionSize(count: Int): Option[TransactionError.TransactionValidationError] =
    if (count < 1 || count > 100)
      Some(
        TransactionError.TransactionValidationError(
          s"Transaction must contain between 1 and 100 items; got $count"
        )
      )
    else None

  // Returns Some(error) if any item is not a valid transact write operation.
  private def validateTransactWriteItems(
    items: Chunk[DynamoDBQuery[Any, Any]]
  ): Option[TransactionError.TransactionValidationError] = {
    val invalid = items.filter {
      case _: DynamoDBQuery.PutItem        => false
      case _: DynamoDBQuery.UpdateItem     => false
      case _: DynamoDBQuery.DeleteItem     => false
      case _: DynamoDBQuery.ConditionCheck => false
      case _                               => true
    }
    if (invalid.isEmpty) None
    else
      Some(
        TransactionError.TransactionValidationError(
          s"transactWriteItems only accepts putItem, updateItem, deleteItem, conditionCheck; " +
            s"got: ${invalid.map(_.getClass.getSimpleName).mkString(", ")}"
        )
      )
  }

  // Builds the onRetry closure RetryInterceptor.onRetry threads through withRetry/withRetryF —
  // a no-op F[Unit] when no RetryInterceptor is attached, so callers never branch on it.
  private def onRetryFor(meta: DynamoDBRetryMetadata): (Throwable, Int) => F[Unit] =
    (t, n) => retryInterceptor.fold(pure(()))(_.onRetry(meta, t, n))

  // Works on Any to sidestep Scala 2 GADT limitations; safe by construction.
  private def runAny(query: DynamoDBQuery[_, _]): F[Any] =
    query match {
      case q: DynamoDBQuery.GetItem            =>
        val meta = DynamoDBRetryMetadata.GetItem(q.tableName, CorrelationContext(Some(q.key)))
        withOptionalRetry(q.retryPolicy, onRetryFor(meta))(runGetItem(q)).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.PutItem            =>
        val meta = DynamoDBRetryMetadata.PutItem(q.tableName, q.item)
        validateCE(q.conditionExpression).fold(
          withOptionalRetry(q.retryPolicy, onRetryFor(meta))(runPutItem(q)).asInstanceOf[F[Any]]
        )(fail)
      case q: DynamoDBQuery.UpdateItem         =>
        val meta = DynamoDBRetryMetadata.UpdateItem(q.tableName, CorrelationContext(Some(q.key)))
        validateAction(q.updateExpression.action)
          .orElse(validateCE(q.conditionExpression))
          .fold(
            withExplicitRetryOnly(q.retryPolicy, onRetryFor(meta))(runUpdateItem(q)).asInstanceOf[F[Any]]
          )(fail)
      case q: DynamoDBQuery.DeleteItem         =>
        val meta = DynamoDBRetryMetadata.DeleteItem(q.tableName, CorrelationContext(Some(q.key)))
        validateCE(q.conditionExpression).fold(
          withOptionalRetry(q.retryPolicy, onRetryFor(meta))(runDeleteItem(q)).asInstanceOf[F[Any]]
        )(fail)
      case q: DynamoDBQuery.Query              =>
        val meta = DynamoDBRetryMetadata.Query(q.tableName)
        validateKCE(q.keyConditionExpr)
          .orElse(validateCE(q.filterExpression))
          .fold(
            withOptionalRetry(q.retryPolicy, onRetryFor(meta))(runQuery(q)).asInstanceOf[F[Any]]
          )(fail)
      case q: DynamoDBQuery.Scan               =>
        val meta = DynamoDBRetryMetadata.Scan(q.tableName)
        validateCE(q.filterExpression)
          .orElse(validateScanSegment(q))
          .fold(
            withOptionalRetry(q.retryPolicy, onRetryFor(meta))(runScan(q)).asInstanceOf[F[Any]]
          )(err => fail(err))
      case q: DynamoDBQuery.CreateTable        => runCreateTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.DeleteTable        => runDeleteTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.DescribeTable      => runDescribeTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.BatchGetItem       =>
        flatMap(newResponseLevelDelayFn(q.retryPolicy))(getDelay => runBatchGetItemRetrying(q, getDelay, attempt = 0))
          .asInstanceOf[F[Any]]
      case q: DynamoDBQuery.BatchWriteItem     =>
        flatMap(newResponseLevelDelayFn(q.retryPolicy))(getDelay => runBatchWriteItemRetrying(q, getDelay, attempt = 0))
          .asInstanceOf[F[Any]]
      case q: DynamoDBQuery.TransactGetItems   =>
        validateTransactionSize(q.getItems.length).fold(
          runTransactGetItems(q).asInstanceOf[F[Any]]
        )(err => fail(err))
      case q: DynamoDBQuery.TransactWriteItems =>
        validateTransactionSize(q.writeItems.length)
          .orElse(validateTransactWriteItems(q.writeItems))
          .fold(runTransactWriteItems(q).asInstanceOf[F[Any]])(err => fail(err))
      case _: DynamoDBQuery.ConditionCheck     =>
        // ConditionCheck is only valid inside TransactWriteItems, never as a standalone query.
        fail(
          DynamoDBError.TransactionError.TransactionValidationError(
            "ConditionCheck cannot be executed as a standalone query; use transactWriteItems"
          )
        )
      case z: DynamoDBQuery.ZipPar[_, _, _]    =>
        map(productPar(runAny(z.left), runAny(z.right)))(pair =>
          z.zippable.zip(pair._1.asInstanceOf[z.Left], pair._2.asInstanceOf[z.Right])
        )
      case m: DynamoDBQuery.Map[_, _]          =>
        map(runAny(m.query))(m.mapper.asInstanceOf[Any => Any])
      case DynamoDBQuery.Succeed(v)            => pure(v())
      case DynamoDBQuery.Fail(e)               => fail(e())
      case a: DynamoDBQuery.Absolve[_, _]      =>
        absolve(runAny(a.query).asInstanceOf[F[Either[ItemError, Any]]])
    }

  // Drives both the effect-level retry (transient failures, via withOptionalRetryTracked) and
  // the response-level retry (resubmitting unprocessedKeys/unprocessedItems) for batch queries,
  // so `run` alone is sufficient — no separate entry point needed for batch vs. everything
  // else. `getDelay` (resolved once per batch call, see `newResponseLevelDelayFn`) governs the
  // response-level loop; the effect-level loop re-resolves its own retry policy fresh on each
  // resubmission via `q.retryPolicy` (unchanged by `q.copy`, so this is stable across a call).
  // `accumulatedResponses` carries item data recovered in earlier attempts forward — each
  // retry only re-requests the residual unprocessedKeys, so its response alone would
  // otherwise "forget" items already fetched in prior attempts.
  private def runBatchGetItemRetrying(
    q: DynamoDBQuery.BatchGetItem,
    getDelay: Int => F[Option[FiniteDuration]],
    attempt: Int,
    accumulatedResponses: Map[String, Chunk[Item]] = Map.empty
  ): F[Batch.GetResult] = {
    val meta = DynamoDBRetryMetadata.BatchGetItem(q.requestItems.keySet)
    flatMap(withOptionalRetryTracked(q.retryPolicy, onRetryFor(meta))(runBatchGetItem(q))) {
      case Left((cause, effectRetries)) =>
        pure(Batch.GetResult.Failed(cause, responseRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        val merged = response.responses.foldLeft(accumulatedResponses) { case (acc, (tableName, items)) =>
          acc.updated(tableName, acc.getOrElse(tableName, Chunk.empty) ++ items)
        }
        if (response.unprocessedKeys.isEmpty)
          pure(Batch.GetResult.Complete(response.copy(responses = merged)))
        else
          flatMap(getDelay(attempt)) {
            case None    => pure(Batch.GetResult.Incomplete(response.copy(responses = merged)))
            case Some(d) =>
              val unprocessed: Map[String, Set[PrimaryKey]] =
                response.unprocessedKeys.map { case (tableName, tableGet) => tableName -> tableGet.keysSet }
              val notify: F[Unit]                           =
                batchRetryInterceptor.fold(pure(()))(_.onBatchGetRetry(unprocessed, attempt))
              flatMap(notify) { _ =>
                flatMap(sleep(d)) { _ =>
                  // q.copy (not a fresh BatchGetItem) preserves capacity/orderedGetItems/
                  // retryPolicy from the original query across the retry.
                  runBatchGetItemRetrying(
                    q.copy(requestItems = response.unprocessedKeys),
                    getDelay,
                    attempt + 1,
                    merged
                  )
                }
              }
          }
    }
  }

  private def runBatchWriteItemRetrying(
    q: DynamoDBQuery.BatchWriteItem,
    getDelay: Int => F[Option[FiniteDuration]],
    attempt: Int
  ): F[Batch.WriteResult] = {
    val meta = DynamoDBRetryMetadata.BatchWriteItem(q.requestItems.keySet)
    flatMap(withOptionalRetryTracked(q.retryPolicy, onRetryFor(meta))(runBatchWriteItem(q))) {
      case Left((cause, effectRetries)) =>
        pure(Batch.WriteResult.Failed(cause, responseRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        response.unprocessedItems match {
          case None            => pure(Batch.WriteResult.Complete(response))
          case Some(remaining) =>
            flatMap(getDelay(attempt)) {
              case None    => pure(Batch.WriteResult.Incomplete(response))
              case Some(d) =>
                // Splits each table's order-preserving Chunk[Write] into separate puts/deletes
                // views for the interceptor — Chunk, not Set, to match onBatchWriteRetry's
                // no-implicit-dedup contract (BatchRetryInterceptor.scala).
                val (puts, deletes) = remaining.foldLeft(
                  (Map.empty[String, Chunk[Item]], Map.empty[String, Chunk[PrimaryKey]])
                ) { case ((putsAcc, deletesAcc), (tableName, writes)) =>
                  val tablePuts    = writes.collect { case DynamoDBQuery.BatchWriteItem.Put(item) => item }
                  val tableDeletes = writes.collect { case DynamoDBQuery.BatchWriteItem.Delete(key) => key }
                  (
                    if (tablePuts.isEmpty) putsAcc else putsAcc.updated(tableName, tablePuts),
                    if (tableDeletes.isEmpty) deletesAcc else deletesAcc.updated(tableName, tableDeletes)
                  )
                }
                val notify: F[Unit] =
                  batchRetryInterceptor.fold(pure(()))(_.onBatchWriteRetry(puts, deletes, attempt))
                flatMap(notify) { _ =>
                  flatMap(sleep(d)) { _ =>
                    // q.copy (not a fresh BatchWriteItem) preserves capacity/itemMetrics/
                    // retryPolicy from the original query across the retry.
                    runBatchWriteItemRetrying(
                      q.copy(requestItems = remaining),
                      getDelay,
                      attempt + 1
                    )
                  }
                }
            }
        }
    }
  }

  final def run[Out](query: DynamoDBQuery[_, Out]): F[Out] =
    runAny(query).asInstanceOf[F[Out]]
}

// -- DummyIO interpreter -------------------------------------------------
// Stub interpreter extending AwsInterpreter directly with hardcoded no-ops.
// No AWS SDK types involved; used for testing query-composition logic.
// class DummyIOInterpreter(client) — extends RealAwsInterpreter, lives in aws/test.

object DummyIOInterpreter extends AwsInterpreter[DummyIO] {

  private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
  private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] = fa.flatMap(f)
  protected def product[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]        =
    DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
  protected def productPar[A, B](fa: DummyIO[A], fb: DummyIO[B]): DummyIO[(A, B)]     =
    DummyIO(() => (fa.unsafeRun(), fb.unsafeRun()))
  protected def fail[A](e: DynamoDBError): DummyIO[A]                                 =
    DummyIO(() => throw e)
  protected def absolve[A](fa: DummyIO[Either[ItemError, A]]): DummyIO[A]             =
    DummyIO(() =>
      fa.unsafeRun() match {
        case Right(a) => a
        case Left(e)  => throw e
      }
    )

  // sleep is a no-op in tests; attempt/raiseError wrap synchronous throws
  private[dynamodb] def sleep(d: FiniteDuration): DummyIO[Unit]                   =
    DummyIO.succeed(())
  private[dynamodb] def attempt[A](fa: DummyIO[A]): DummyIO[Either[Throwable, A]] =
    DummyIO(() => scala.util.Try(fa.unsafeRun()).toEither)
  private[dynamodb] def raiseError[A](t: Throwable): DummyIO[A]                   =
    DummyIO(() => throw t)

  protected def runGetItem(q: DynamoDBQuery.GetItem): DummyIO[Option[Item]]                                        = DummyIO.succeed(None)
  protected def runPutItem(q: DynamoDBQuery.PutItem): DummyIO[Option[Item]]                                        = DummyIO.succeed(None)
  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): DummyIO[Option[Item]]                                  = DummyIO.succeed(None)
  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): DummyIO[Option[Item]]                                  = DummyIO.succeed(None)
  protected def runQuery(q: DynamoDBQuery.Query): DummyIO[Page[Item]]                                              =
    DummyIO.succeed(Page(Chunk.empty, None, 0, 0))
  protected def runScan(q: DynamoDBQuery.Scan): DummyIO[Page[Item]]                                                =
    DummyIO.succeed(Page(Chunk.empty, None, 0, 0))
  protected def runCreateTable(q: DynamoDBQuery.CreateTable): DummyIO[Unit]                                        =
    DummyIO.succeed(())
  protected def runDeleteTable(q: DynamoDBQuery.DeleteTable): DummyIO[Unit]                                        =
    DummyIO.succeed(())
  protected def runDescribeTable(q: DynamoDBQuery.DescribeTable): DummyIO[DynamoDBQuery.DescribeTableResponse]     =
    DummyIO.succeed(DynamoDBQuery.DescribeTableResponse("arn:dummy", DynamoDBQuery.TableStatus.Active, 0L, 0L))
  protected def runBatchGetItem(q: DynamoDBQuery.BatchGetItem): DummyIO[DynamoDBQuery.BatchGetItem.Response]       =
    DummyIO.succeed(DynamoDBQuery.BatchGetItem.Response())
  protected def runBatchWriteItem(q: DynamoDBQuery.BatchWriteItem): DummyIO[DynamoDBQuery.BatchWriteItem.Response] =
    DummyIO.succeed(DynamoDBQuery.BatchWriteItem.Response(None))
  protected def runTransactGetItems(q: DynamoDBQuery.TransactGetItems): DummyIO[Chunk[Option[Item]]]               =
    DummyIO.succeed(Chunk.fill(q.getItems.length)(None))
  protected def runTransactWriteItems(q: DynamoDBQuery.TransactWriteItems): DummyIO[Unit]                          =
    DummyIO.succeed(())
}
