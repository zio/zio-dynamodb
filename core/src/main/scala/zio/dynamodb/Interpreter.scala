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
   * [[RetryQuota]] token cost for an error `isRetryable` already matched. `core` has no AWS SDK
   *  dependency, so this defaults to [[RetryPolicy.retryCost]]'s message-substring check;
   *  `RealAwsInterpreter` (in `aws`) overrides it the same way `isRetryable` is overridden.
   */
  protected def retryCost: Throwable => Int = RetryPolicy.retryCost

  /**
   * The fallback used when a query doesn't specify its own `.withRetryPolicy(...)`. This
   *  trait's own default is `None` (no retry); concrete interpreters override it via a
   *  `fromAsyncClient` factory parameter rather than subclassing, so a caller can swap or
   *  disable it in one line without touching interpreter internals.
   */
  protected def defaultRetryPolicy: Option[EffectfulRetryPolicy[F]] = None

  /**
   * The policy for resubmitting a batch's unprocessed keys / items when the batch call doesn't set
   *  `.withBatchUnprocessedRetryPolicy(...)`. Independent of `defaultRetryPolicy` / `.withRetryPolicy`,
   *  which only govern retries of failed requests. This trait's own default is `None`; the concrete
   *  interpreters default it to [[RetryPolicy.fullJitter]] via a `fromAsyncClient` factory parameter.
   */
  protected def defaultBatchUnprocessedRetryPolicy: Option[RetryPolicy] = None

  /**
   * Fires just before an effect-level retry sleeps and retries — see [[RetryInterceptor]]. This
   *  trait's own default is `None`; concrete interpreters override it the same way as
   *  `defaultRetryPolicy`, via a `fromAsyncClient` factory parameter.
   */
  protected def retryInterceptor: Option[RetryInterceptor[F]] = None

  /**
   * Fires just before the batch unprocessed retry sleeps and
   *  resubmits — see [[BatchUnprocessedRetryInterceptor]]. Independent of `retryInterceptor`: the
   *  batch unprocessed retry resubmits from a successful response with no `Throwable` involved, a
   *  genuinely different mechanism from the effect-level retry loop `retryInterceptor` covers,
   *  so a caller who only wants effect-level retry visibility has nothing to no-op here.
   */
  protected def batchUnprocessedRetryInterceptor: Option[BatchUnprocessedRetryInterceptor[F]] = None

  /**
   * Client-scoped circuit breaker gating whether a retry should happen at all — see
   *  [[RetryQuota]]. This trait's own default is `None`; concrete interpreters override it the
   *  same way as `defaultRetryPolicy`, via a `fromAsyncClient` factory parameter. Also charged by
   *  the batch unprocessed retry (5 tokens per resubmission); an exhausted quota ends it with
   *  `Incomplete`.
   */
  protected def retryQuota: Option[RetryQuota[F]] = None

  // A denied retry (quota exhausted) looks exactly like curve-exhaustion to the caller — no
  // retry, no onRetry fired.
  private def tryConsumeQuota(quota: Option[RetryQuota[F]], cost: Int): F[Boolean] =
    quota.fold(pure(true))(_.tryConsume(cost))

  // AWS's own rule: a clean (no-retry) success credits 1 token; a success after `attempts`
  // retries credits back exactly what those retries cost (`spent`), no more.
  private def creditQuota(quota: Option[RetryQuota[F]], attempts: Int, spent: Int): F[Unit] =
    quota.fold(pure(()))(q => q.credit(if (attempts == 0) 1 else spent))

  // Unprocessed keys / items are DynamoDB's throttling signal for batches, so each resubmission
  // costs the throttling rate. On completion the resubmissions' cost is credited back; the clean
  // first request's 1-token credit is already given by the effect-level loop around it.
  private val UnprocessedRetryCost = 5

  private def creditUnprocessedRetries[A](attempt: Int, a: A): F[A] =
    if (attempt == 0) pure(a)
    else retryQuota.fold(pure(a))(q => map(q.credit(attempt * UnprocessedRetryCost))(_ => a))

  /**
   * Retries `fa` according to `policy` whenever `isRetryable` matches the
   *  thrown error. Exhausted retries re-raise the last error.
   *
   *  `fa` is by-name so each retry re-evaluates the effect. `onRetry` fires once per retried
   *  attempt, right before the delay is slept; defaults to a no-op so every existing caller
   *  (this method is public) keeps compiling unchanged. `retryQuota`, if attached, is checked
   *  before `onRetry` fires on each retry attempt and credited on eventual success.
   */
  final def withRetry[A](
    policy: RetryPolicy,
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(()),
    retryCost: Throwable => Int = RetryPolicy.retryCost,
    retryQuota: Option[RetryQuota[F]] = None
  )(fa: => F[A]): F[A] =
    // newAttempt() is deferred into the flatMap continuation so a reused F[A] value (e.g. a
    // caller holding `val effect = interp.run(query)` and running it more than once) gets a
    // fresh Attempt on every execution, not one shared across all of them.
    flatMap(pure(())) { _ =>
      val attemptState                   = policy.newAttempt()
      def loop(n: Int, spent: Int): F[A] =
        flatMap(attempt(fa)) {
          case Right(a)                  => flatMap(creditQuota(retryQuota, n, spent))(_ => pure(a))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            attemptState.nextDelay(n) match {
              case None    => raiseError(t)
              case Some(d) =>
                val cost = retryCost(t)
                flatMap(tryConsumeQuota(retryQuota, cost)) {
                  case false => raiseError(t)
                  case true  => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1, spent + cost)))
                }
            }
          case Left(t)                   => raiseError(t)
        }
      loop(0, 0)
    }

  /**
   * Like [[withRetry]] but returns the number of effect-level retries made alongside
   *  the result. On success yields `Right(a)`. On exhaustion yields `Left((cause, retries))`
   *  where `retries` is the number of retries attempted (0 = failed on first call).
   */
  private[dynamodb] final def withRetryTracked[A](
    policy: RetryPolicy,
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(()),
    retryCost: Throwable => Int = RetryPolicy.retryCost,
    retryQuota: Option[RetryQuota[F]] = None
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] =
    // See withRetry — newAttempt() deferred so a reused F[A] value gets a fresh Attempt per run.
    flatMap(pure(())) { _ =>
      val attemptState                                             = policy.newAttempt()
      def loop(n: Int, spent: Int): F[Either[(Throwable, Int), A]] =
        flatMap(attempt(fa)) {
          case Right(a)                  => flatMap(creditQuota(retryQuota, n, spent))(_ => pure(Right(a)))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            attemptState.nextDelay(n) match {
              case None    => pure(Left((t, n)))
              case Some(d) =>
                val cost = retryCost(t)
                flatMap(tryConsumeQuota(retryQuota, cost)) {
                  case false => pure(Left((t, n)))
                  case true  => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1, spent + cost)))
                }
            }
          case Left(t)                   => pure(Left((t, n)))
        }
      loop(0, 0)
    }

  /** Like [[withRetry]], but for an [[EffectfulRetryPolicy]] — see `defaultRetryPolicy`. */
  final def withRetryF[A](
    policy: EffectfulRetryPolicy[F],
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(()),
    retryCost: Throwable => Int = RetryPolicy.retryCost,
    retryQuota: Option[RetryQuota[F]] = None
  )(fa: => F[A]): F[A] =
    flatMap(policy.newAttempt()) { attemptState =>
      def loop(n: Int, spent: Int): F[A] =
        flatMap(attempt(fa)) {
          case Right(a)                  => flatMap(creditQuota(retryQuota, n, spent))(_ => pure(a))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            flatMap(attemptState.nextDelay(n)) {
              case None    => raiseError(t)
              case Some(d) =>
                val cost = retryCost(t)
                flatMap(tryConsumeQuota(retryQuota, cost)) {
                  case false => raiseError(t)
                  case true  => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1, spent + cost)))
                }
            }
          case Left(t)                   => raiseError(t)
        }
      loop(0, 0)
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
      case Some(p) => withRetry(p, isRetryable, onRetry, retryCost, retryQuota)(fa)
      case None    =>
        defaultRetryPolicy match {
          case Some(p) => withRetryF(p, isRetryable, onRetry, retryCost, retryQuota)(fa)
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
   *  `retryQuota` still applies when a query opts in explicitly — it's a client-wide circuit
   *  breaker, not tied to any one operation's retry-fallback eligibility.
   */
  private def withExplicitRetryOnly[A](
    retryPolicy: Option[RetryPolicy],
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(())
  )(fa: => F[A]): F[A] =
    retryPolicy match {
      case Some(p) => withRetry(p, isRetryable, onRetry, retryCost, retryQuota)(fa)
      case None    => fa
    }

  /** Like [[withRetryTracked]], but for an [[EffectfulRetryPolicy]] — see `defaultRetryPolicy`. */
  private[dynamodb] final def withRetryTrackedF[A](
    policy: EffectfulRetryPolicy[F],
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable,
    onRetry: (Throwable, Int) => F[Unit] = (_, _) => pure(()),
    retryCost: Throwable => Int = RetryPolicy.retryCost,
    retryQuota: Option[RetryQuota[F]] = None
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] =
    flatMap(policy.newAttempt()) { attemptState =>
      def loop(n: Int, spent: Int): F[Either[(Throwable, Int), A]] =
        flatMap(attempt(fa)) {
          case Right(a)                  => flatMap(creditQuota(retryQuota, n, spent))(_ => pure(Right(a)))
          case Left(t) if !NonFatal(t)   => raiseError(t)
          case Left(t) if isRetryable(t) =>
            flatMap(attemptState.nextDelay(n)) {
              case None    => pure(Left((t, n)))
              case Some(d) =>
                val cost = retryCost(t)
                flatMap(tryConsumeQuota(retryQuota, cost)) {
                  case false => pure(Left((t, n)))
                  case true  => flatMap(onRetry(t, n))(_ => flatMap(sleep(d))(_ => loop(n + 1, spent + cost)))
                }
            }
          case Left(t)                   => pure(Left((t, n)))
        }
      loop(0, 0)
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
      case Some(p) => withRetryTracked(p, isRetryable, onRetry, retryCost, retryQuota)(fa)
      case None    =>
        defaultRetryPolicy match {
          case Some(p) => withRetryTrackedF(p, isRetryable, onRetry, retryCost, retryQuota)(fa)
          case None    => withRetryTracked(RetryPolicy.NoRetry, isRetryable, onRetry, retryCost, retryQuota)(fa)
        }
    }

  /**
   * The delay-decision function for the batch unprocessed retry - resolved once per batch call from
   * `unprocessedRetryPolicy` (`.withBatchUnprocessedRetryPolicy`), else
   * `defaultBatchUnprocessedRetryPolicy`; never from `.withRetryPolicy` / `defaultRetryPolicy`, which
   * govern only retries of failed requests. Threaded through the loop's recursion so a stateful
   * policy only calls `newAttempt()` once per batch call, not once per resubmission.
   */
  private def newUnprocessedDelayFn(policy: Option[RetryPolicy]): F[Int => F[Option[FiniteDuration]]] =
    policy.orElse(defaultBatchUnprocessedRetryPolicy) match {
      case Some(p) =>
        // See withRetry — newAttempt() deferred so a reused F[...] value gets a fresh Attempt per run.
        flatMap(pure(())) { _ =>
          val attemptState = p.newAttempt()
          pure((n: Int) => pure(attemptState.nextDelay(n)))
        }
      case None    => pure((_: Int) => pure(None))
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

  // Returns Some(accumulated DecodingError) if any write item contains a Failure CE, or (for
  // an UpdateItem) a Failure node in its update action — the same two checks a standalone
  // updateItem gets from validateAction/validateCE in the UpdateItem case below. Without the
  // action check, a transactional update whose action is Action.Failure (e.g. an HL `update`
  // value built from an unrepresentable optic path) would reach toAwsTransactWriteItem and
  // render as an empty or partially filtered update instead of failing before any AWS call.
  private def validateTransactWriteItemsFailures(
    items: Chunk[DynamoDBQuery[Any, Any]]
  ): Option[DynamoDBError] = {
    val errors: List[String] = items.toList.flatMap {
      case p: DynamoDBQuery.PutItem        => ConditionExpression.collectFailures(p.conditionExpression)
      case u: DynamoDBQuery.UpdateItem     =>
        UpdateExpression.collectFailures(u.updateExpression.action) ++
          ConditionExpression.collectFailures(u.conditionExpression)
      case d: DynamoDBQuery.DeleteItem     => ConditionExpression.collectFailures(d.conditionExpression)
      case c: DynamoDBQuery.ConditionCheck => ConditionExpression.collectFailures(c.conditionExpression)
      case _                               => Nil
    }
    accumulateErrors(errors)
  }

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

  // TransactWriteItems has no per-item ReturnValues, so a write asking for one would otherwise be
  // sent without it and silently return nothing.
  private def validateTransactWriteReturnValues(
    items: Chunk[DynamoDBQuery[Any, Any]]
  ): Option[TransactionError.TransactionValidationError] = {
    val withReturnValues = items.collect {
      case p: DynamoDBQuery.PutItem if p.returnValues != ReturnValues.None    => "putItem"
      case u: DynamoDBQuery.UpdateItem if u.returnValues != ReturnValues.None => "updateItem"
      case d: DynamoDBQuery.DeleteItem if d.returnValues != ReturnValues.None => "deleteItem"
    }
    if (withReturnValues.isEmpty) None
    else
      Some(
        TransactionError.TransactionValidationError(
          "transactWriteItems returns no items, so ReturnValues (.returning) is not supported " +
            s"inside a transaction; got it on: ${withReturnValues.mkString(", ")}"
        )
      )
  }

  // A transaction runs as one request and encodes only each item's target, expressions and
  // returnValuesOnConditionCheckFailure; capacity and metrics are set on the transaction itself.
  // A get item's consistency isn't checked: transactional reads are serializable, which satisfies it.
  private def validateTransactItemModifiers(
    items: Chunk[DynamoDBQuery[Any, Any]]
  ): Option[DynamoDBError.QueryBuilderError.UnsupportedModifier] = {
    def write(
      name: String,
      retryPolicy: Option[RetryPolicy],
      capacity: ReturnConsumedCapacity,
      itemMetrics: ReturnItemCollectionMetrics
    ): List[String] =
      (if (retryPolicy.isDefined) List(s"$name.withRetryPolicy") else Nil) ++
        (if (capacity != ReturnConsumedCapacity.None) List(s"$name.capacity") else Nil) ++
        (if (itemMetrics != ReturnItemCollectionMetrics.None) List(s"$name.metrics") else Nil)
    val ignored     = items.toList.flatMap {
      case g: DynamoDBQuery.GetItem    =>
        (if (g.retryPolicy.isDefined) List("getItem.withRetryPolicy") else Nil) ++
          (if (g.capacity != ReturnConsumedCapacity.None) List("getItem.capacity") else Nil)
      case p: DynamoDBQuery.PutItem    => write("putItem", p.retryPolicy, p.capacity, p.itemMetrics)
      case u: DynamoDBQuery.UpdateItem => write("updateItem", u.retryPolicy, u.capacity, u.itemMetrics)
      case d: DynamoDBQuery.DeleteItem => write("deleteItem", d.retryPolicy, d.capacity, d.itemMetrics)
      case _                           => Nil
    }
    if (ignored.isEmpty) None
    else {
      val hasRetry = ignored.exists(_.endsWith(".withRetryPolicy"))
      val hasOther = ignored.exists(m => !m.endsWith(".withRetryPolicy"))
      val remedies =
        (if (hasOther) List("set capacity / metrics on the transaction itself") else Nil) ++
          (if (hasRetry) List("zio-dynamodb doesn't retry transactions (the AWS SDK's own retries still apply)")
           else Nil)
      Some(
        DynamoDBError.QueryBuilderError.UnsupportedModifier(
          s"a transaction runs as one request, so these item-level modifiers have no effect: ${ignored.mkString(", ")}" +
            s"; ${remedies.mkString("; ")}"
        )
      )
    }
  }

  // Builds the onRetry closure RetryInterceptor.onRetry threads through withRetry/withRetryF —
  // a no-op F[Unit] when no RetryInterceptor is attached, so callers never branch on it.
  private def onRetryFor(meta: DynamoDBRetryMetadata): (Throwable, Int) => F[Unit] =
    (t, n) => retryInterceptor.fold(pure(()))(_.onRetry(meta, t, n))

  // Unwraps a High-Level write value to its leaf (PutItem/UpdateItem/DeleteItem/ConditionCheck);
  // a Fail short-circuits as the error.
  @scala.annotation.tailrec
  private def unwrapMappedWriteItem(q: DynamoDBQuery[Any, Any]): Either[DynamoDBError, DynamoDBQuery[Any, Any]] =
    q match {
      case DynamoDBQuery.Map(inner, _)  => unwrapMappedWriteItem(inner.asInstanceOf[DynamoDBQuery[Any, Any]])
      case w: WriteNode[_, _]           => unwrapMappedWriteItem(w.write.asInstanceOf[DynamoDBQuery[Any, Any]])
      case c: ConditionCheckBuilder[_]  => unwrapMappedWriteItem(c.check.asInstanceOf[DynamoDBQuery[Any, Any]])
      case DynamoDBQuery.Absolve(inner) => unwrapMappedWriteItem(inner.asInstanceOf[DynamoDBQuery[Any, Any]])
      case f: DynamoDBQuery.Fail        => Left(f.error())
      case other                        => Right(other)
    }

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
        q.itemError match {
          case Some(err) => fail(err)
          case None      =>
            flatMap(newUnprocessedDelayFn(q.unprocessedRetryPolicy))(getDelay =>
              runBatchGetItemRetrying(q, getDelay, attempt = 0)
            ).asInstanceOf[F[Any]]
        }
      case q: DynamoDBQuery.BatchWriteItem     =>
        q.itemError match {
          case Some(err) => fail(err)
          case None      =>
            flatMap(newUnprocessedDelayFn(q.unprocessedRetryPolicy))(getDelay =>
              runBatchWriteItemRetrying(q, getDelay, attempt = 0)
            ).asInstanceOf[F[Any]]
        }
      case q: DynamoDBQuery.TransactGetItems   =>
        validateTransactionSize(q.getItems.length)
          .orElse(validateTransactItemModifiers(q.getItems))
          .fold(runTransactGetItems(q).asInstanceOf[F[Any]])(err => fail(err))
      case q: DynamoDBQuery.TransactWriteItems =>
        q.writeItems.foldLeft[Either[DynamoDBError, Chunk[DynamoDBQuery[Any, Any]]]](Right(Chunk.empty)) { (acc, item) =>
          acc.flatMap(leaves => unwrapMappedWriteItem(item).map(leaves :+ _))
        } match {
          case Left(err)     => fail(err)
          case Right(leaves) =>
            val unwrapped = q.copy(writeItems = leaves)
            validateTransactionSize(unwrapped.writeItems.length)
              .orElse(validateTransactWriteItems(unwrapped.writeItems))
              .orElse(validateTransactWriteReturnValues(unwrapped.writeItems))
              .orElse(validateTransactItemModifiers(unwrapped.writeItems))
              .orElse(validateTransactWriteItemsFailures(unwrapped.writeItems))
              .fold(runTransactWriteItems(unwrapped).asInstanceOf[F[Any]])(err => fail(err))
        }
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
      case w: WriteNode[_, _]                  =>
        map(runAny(w.write))(w.finish.asInstanceOf[Any => Any])
      case c: ConditionCheckBuilder[_]         => runAny(c.check)
      case DynamoDBQuery.Succeed(v)            => pure(v())
      case DynamoDBQuery.Fail(e)               => fail(e())
      case a: DynamoDBQuery.Absolve[_, _]      =>
        absolve(runAny(a.query).asInstanceOf[F[Either[ItemError, Any]]])
    }

  // Drives both the effect-level retry (transient failures, via withOptionalRetryTracked) and
  // the batch unprocessed retry (resubmitting unprocessedKeys/unprocessedItems) for batch queries,
  // so `run` alone is sufficient — no separate entry point needed for batch vs. everything
  // else. `getDelay` (resolved once per batch call, see `newUnprocessedDelayFn`) governs the
  // batch unprocessed retry; the effect-level loop re-resolves its own retry policy fresh on each
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
        pure(Batch.GetResult.Failed(cause, unprocessedRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        val merged = response.responses.foldLeft(accumulatedResponses) { case (acc, (tableName, items)) =>
          acc.updated(tableName, acc.getOrElse(tableName, Chunk.empty) ++ items)
        }
        if (response.unprocessedKeys.isEmpty)
          creditUnprocessedRetries(attempt, Batch.GetResult.Complete(response.copy(responses = merged)))
        else {
          def incomplete = pure[Batch.GetResult](Batch.GetResult.Incomplete(response.copy(responses = merged)))
          flatMap(getDelay(attempt)) {
            case None    => incomplete
            case Some(d) =>
              flatMap(tryConsumeQuota(retryQuota, UnprocessedRetryCost)) { granted =>
                if (!granted) incomplete
                else {
                  val unprocessed: Map[String, Set[PrimaryKey]] =
                    response.unprocessedKeys.map { case (tableName, tableGet) => tableName -> tableGet.keysSet }
                  val notify: F[Unit]                           =
                    batchUnprocessedRetryInterceptor.fold(pure(()))(_.onUnprocessedKeysRetry(unprocessed, attempt))
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
        pure(Batch.WriteResult.Failed(cause, unprocessedRetries = attempt, effectRetries = effectRetries))
      case Right(response)              =>
        response.unprocessedItems match {
          case None            => creditUnprocessedRetries(attempt, Batch.WriteResult.Complete(response))
          case Some(remaining) =>
            flatMap(getDelay(attempt)) {
              case None    => pure(Batch.WriteResult.Incomplete(response))
              case Some(d) =>
                flatMap(tryConsumeQuota(retryQuota, UnprocessedRetryCost)) { granted =>
                  if (!granted) pure(Batch.WriteResult.Incomplete(response))
                  else {
                    // Splits each table's order-preserving Chunk[Write] into separate puts/deletes
                    // views for the interceptor — Chunk, not Set, to match onUnprocessedItemsRetry's
                    // no-implicit-dedup contract (BatchUnprocessedRetryInterceptor.scala).
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
                      batchUnprocessedRetryInterceptor.fold(pure(()))(_.onUnprocessedItemsRetry(puts, deletes, attempt))
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
