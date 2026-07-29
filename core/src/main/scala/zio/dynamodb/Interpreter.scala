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
   * Retries `fa` according to `policy` whenever `isRetryable` matches the
   *  thrown error. Exhausted retries re-raise the last error.
   *
   *  `fa` is by-name so each retry re-evaluates the effect.
   */
  final def withRetry[A](
    policy: RetryPolicy,
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable
  )(fa: => F[A]): F[A] = {
    def loop(n: Int): F[A] =
      flatMap(attempt(fa)) {
        case Right(a)                  => pure(a)
        case Left(t) if !NonFatal(t)   => raiseError(t)
        case Left(t) if isRetryable(t) =>
          policy.nextDelay(n) match {
            case None    => raiseError(t)
            case Some(d) => flatMap(sleep(d))(_ => loop(n + 1))
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
    isRetryable: Throwable => Boolean = RetryPolicy.isRetryable
  )(fa: => F[A]): F[Either[(Throwable, Int), A]] = {
    def loop(n: Int): F[Either[(Throwable, Int), A]] =
      flatMap(attempt(fa)) {
        case Right(a)                  => pure(Right(a))
        case Left(t) if !NonFatal(t)   => raiseError(t)
        case Left(t) if isRetryable(t) =>
          policy.nextDelay(n) match {
            case None    => pure(Left((t, n)))
            case Some(d) => flatMap(sleep(d))(_ => loop(n + 1))
          }
        case Left(t)                   => pure(Left((t, n)))
      }
    loop(0)
  }

  // Per-operation methods — each concrete interpreter or stub implements these.
  // In the aws submodule, RealAwsInterpreter provides default impls via AwsCodecs + AwsDynamoDB.
  protected def runGetItem(q: DynamoDBQuery.GetItem): F[Option[Item]]
  protected def runPutItem(q: DynamoDBQuery.PutItem): F[Option[Item]]
  protected def runUpdateItem(q: DynamoDBQuery.UpdateItem): F[Option[Item]]
  protected def runDeleteItem(q: DynamoDBQuery.DeleteItem): F[Option[Item]]
  protected def runQuerySomeItem(q: DynamoDBQuery.QuerySome): F[Page[Item]]
  protected def runScanSome(q: DynamoDBQuery.ScanSome): F[Page[Item]]
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
    kce.flatMap {
      case KeyConditionExpr.Failure(msg) => Some(DynamoDBError.ItemError.DecodingError.failure(msg))
      case _                             => None
    }

  private def validateAction(action: UpdateExpression.Action[_]): Option[DynamoDBError] =
    accumulateErrors(UpdateExpression.collectFailures(action))

  // Returns Some(error) if segment fields are invalid, None if valid.
  private def validateScanSegment(q: DynamoDBQuery.ScanSome): Option[ScanError.ScanValidationError] =
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

  // Returns Some(accumulated DecodingError) if any write item contains a Failure CE.
  private def validateTransactWriteItemsCEs(
    items: Chunk[DynamoDBQuery[Any, Any]]
  ): Option[DynamoDBError] = {
    val errors: List[String] = items.toList.flatMap {
      case p: DynamoDBQuery.PutItem        => ConditionExpression.collectFailures(p.conditionExpression)
      case u: DynamoDBQuery.UpdateItem     => ConditionExpression.collectFailures(u.conditionExpression)
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

  // Works on Any to sidestep Scala 2 GADT limitations; safe by construction.
  private def runAny(query: DynamoDBQuery[_, _]): F[Any] =
    query match {
      case q: DynamoDBQuery.GetItem            =>
        q.retryPolicy.fold(runGetItem(q))(p => withRetry(p)(runGetItem(q))).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.PutItem            =>
        validateCE(q.conditionExpression).fold(
          q.retryPolicy.fold(runPutItem(q))(p => withRetry(p)(runPutItem(q))).asInstanceOf[F[Any]]
        )(fail)
      case q: DynamoDBQuery.UpdateItem         =>
        validateAction(q.updateExpression.action)
          .orElse(validateCE(q.conditionExpression))
          .fold(
            runUpdateItem(q).asInstanceOf[F[Any]]
          )(fail)
      case q: DynamoDBQuery.DeleteItem         =>
        validateCE(q.conditionExpression).fold(
          q.retryPolicy.fold(runDeleteItem(q))(p => withRetry(p)(runDeleteItem(q))).asInstanceOf[F[Any]]
        )(fail)
      case q: DynamoDBQuery.QuerySome          =>
        validateKCE(q.keyConditionExpr)
          .orElse(validateCE(q.filterExpression))
          .fold(
            runQuerySomeItem(q).asInstanceOf[F[Any]]
          )(fail)
      case q: DynamoDBQuery.ScanSome           =>
        validateCE(q.filterExpression)
          .orElse(validateScanSegment(q))
          .fold(
            runScanSome(q).asInstanceOf[F[Any]]
          )(err => fail(err))
      case q: DynamoDBQuery.CreateTable        => runCreateTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.DeleteTable        => runDeleteTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.DescribeTable      => runDescribeTable(q).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.BatchGetItem       =>
        q.retryPolicy.fold(runBatchGetItem(q))(p => withRetry(p)(runBatchGetItem(q))).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.BatchWriteItem     =>
        q.retryPolicy.fold(runBatchWriteItem(q))(p => withRetry(p)(runBatchWriteItem(q))).asInstanceOf[F[Any]]
      case q: DynamoDBQuery.TransactGetItems   =>
        validateTransactionSize(q.getItems.length).fold(
          runTransactGetItems(q).asInstanceOf[F[Any]]
        )(err => fail(err))
      case q: DynamoDBQuery.TransactWriteItems =>
        validateTransactionSize(q.writeItems.length)
          .orElse(validateTransactWriteItems(q.writeItems))
          .orElse(validateTransactWriteItemsCEs(q.writeItems))
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

  final def run[Out](query: DynamoDBQuery[_, Out]): F[Out] =
    runAny(query).asInstanceOf[F[Out]]
}

// -- DummyIO interpreter -------------------------------------------------
// Stub interpreter extending AwsInterpreter directly with hardcoded no-ops.
// No AWS SDK types involved; used for testing query-composition logic.
// class DummyIOInterpreter(client) — extends RealAwsInterpreter, lives in aws/test.

object DummyIOInterpreter extends AwsInterpreter[DummyIO] {

  private[dynamodb] def pure[A](a: A): DummyIO[A]                                     = DummyIO.succeed(a)
  private[dynamodb] def map[A, B](fa: DummyIO[A])(f: A => B): DummyIO[B]              =
    DummyIO(() => f(fa.unsafeRun()))
  private[dynamodb] def flatMap[A, B](fa: DummyIO[A])(f: A => DummyIO[B]): DummyIO[B] =
    DummyIO(() => f(fa.unsafeRun()).unsafeRun())
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
  protected def runQuerySomeItem(q: DynamoDBQuery.QuerySome): DummyIO[Page[Item]]                                  =
    DummyIO.succeed(Page(Chunk.empty, None, 0, 0))
  protected def runScanSome(q: DynamoDBQuery.ScanSome): DummyIO[Page[Item]]                                        =
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
