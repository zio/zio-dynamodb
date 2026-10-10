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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.dynamodb.DynamoDBError.ItemError

import java.util.concurrent.{ CancellationException, CompletableFuture, CompletionException, ExecutionException }
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class ZioInterpreter(
  client: AwsDynamoDB[Task],
  override protected val defaultRetryPolicy: Option[EffectfulRetryPolicy[Task]] = None,
  override protected val retryInterceptor: Option[RetryInterceptor[Task]] = None,
  override protected val batchRetryInterceptor: Option[BatchRetryInterceptor[Task]] = None,
  override protected val retryQuota: Option[RetryQuota[Task]] = None
) extends RealAwsInterpreter[Task](client) {

  private[dynamodb] def pure[A](a: A): Task[A]                               = ZIO.succeed(a)
  private[dynamodb] def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
  protected def product[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]        = fa.zip(fb)
  protected def productPar[A, B](fa: Task[A], fb: Task[B]): Task[(A, B)]     = fa.zipPar(fb)
  protected def fail[A](e: DynamoDBError): Task[A]                           = ZIO.fail(e)
  protected def absolve[A](fa: Task[Either[ItemError, A]]): Task[A]          =
    fa.flatMap(ZIO.fromEither(_))

  private[dynamodb] def sleep(d: FiniteDuration): Task[Unit]                =
    ZIO.sleep(zio.Duration.fromScala(d))
  private[dynamodb] def attempt[A](fa: Task[A]): Task[Either[Throwable, A]] =
    fa.either
  private[dynamodb] def raiseError[A](t: Throwable): Task[A]                =
    ZIO.fail(t)
}

/**
 * A [[ZioInterpreter]] whose `run`/`.execute` fail with [[DynamoFailure]] in ZIO's typed error
 * channel. Built only via [[ZioInterpreter.fromAsyncClientWithTypedErrors]]: SDK failures are
 * classified where the SDK's future is converted to ZIO, and the library's own errors where it
 * raises them, so successful calls pay nothing extra. `classifyAtRun` re-classifies at `run` for
 * configurations with user-supplied effects (interceptors, an effectful default retry policy),
 * whose failures could otherwise be any `Throwable`.
 */
final class TypedErrorsZioInterpreter private[dynamodb] (
  client: AwsDynamoDB[Task],
  defaultRetryPolicy: Option[EffectfulRetryPolicy[Task]],
  retryInterceptor: Option[RetryInterceptor[Task]],
  batchRetryInterceptor: Option[BatchRetryInterceptor[Task]],
  retryQuota: Option[RetryQuota[Task]],
  classifyAtRun: Boolean
) extends ZioInterpreter(client, defaultRetryPolicy, retryInterceptor, batchRetryInterceptor, retryQuota)
    with NarrowedResult {

  type Result[A] = IO[DynamoFailure, A]

  override def run[Out](query: DynamoDBQuery[_, Out]): IO[DynamoFailure, Out] = {
    val result = super.run(query)
    if (classifyAtRun) result.mapError(DynamoFailure.classify)
    else result.asInstanceOf[IO[DynamoFailure, Out]]
  }

  // A top-level ConditionFailedAsLeft never builds the Either: its condition failure is already
  // classified into DynamoFailure.ConditionFailed at the bridge. Anything composed on top of it
  // (e.g. a .map expecting the Either) takes the general path, which flattens after the fact.
  def runFlattened[Out](
    query: DynamoDBQuery[_, Either[DynamoDBError.ItemError.ConditionFailed, Out]]
  ): IO[DynamoFailure, Out] =
    query match {
      case c: DynamoDBQuery.ConditionFailedAsLeft[_, _] => run(c.query).asInstanceOf[IO[DynamoFailure, Out]]
      case other                                        =>
        run(other).flatMap {
          case Right(a) => ZIO.succeed(a)
          case Left(cf) => ZIO.fail(DynamoFailure.classify(cf))
        }
    }

  override protected def fail[A](e: DynamoDBError): Task[A] = ZIO.fail(DynamoFailure.classify(e))

  override protected def absolve[A](fa: Task[Either[ItemError, A]]): Task[A] =
    fa.flatMap {
      case Right(a) => ZIO.succeed(a)
      case Left(e)  => ZIO.fail(DynamoFailure.classify(e))
    }

  override private[dynamodb] def raiseError[A](t: Throwable): Task[A] = ZIO.fail(DynamoFailure.classify(t))
}

object ZioInterpreter {

  private trait FutureBridge {
    def apply[A](thunk: => CompletableFuture[A]): Task[A]
  }

  private val plainBridge: FutureBridge = new FutureBridge {
    def apply[A](thunk: => CompletableFuture[A]): Task[A] = ZIO.fromCompletableFuture(thunk)
  }

  // Same structure as ZIO's own fromCompletableFuture (fast path when already done, else an
  // interruptible async callback), differing only in classifying failures.
  private val classifyingBridge: FutureBridge = new FutureBridge {
    def apply[A](thunk: => CompletableFuture[A]): Task[A] =
      ZIO.uninterruptibleMask { restore =>
        ZIO.suspendSucceed {
          try {
            val cf = thunk
            if (cf.isDone) unwrapDone(cf)
            else
              restore {
                ZIO.asyncInterrupt[Any, Throwable, A] { cb =>
                  cf.handle[Unit] { (v: A, t: Throwable) =>
                    cb(if (t eq null) Exit.succeed(v) else failed(t))
                  }
                  Left(ZIO.succeed(cf.cancel(false)))
                }
              }
          } catch { case t: Throwable => failed(t) }
        }
      }

    private def unwrapDone[A](cf: CompletableFuture[A]): Task[A] =
      try Exit.succeed(cf.get())
      catch { case t: Throwable => failed(t) }

    private def failed(t: Throwable): Task[Nothing] =
      t match {
        case e: CompletionException                             => ZIO.fail(DynamoFailure.classify(e.getCause))
        case e: ExecutionException                              => ZIO.fail(DynamoFailure.classify(e.getCause))
        case _: InterruptedException | _: CancellationException => ZIO.interrupt
        case e if NonFatal(e)                                   => ZIO.fail(DynamoFailure.classify(e))
        case e                                                  => ZIO.die(e)
      }
  }

  /**
   * Creates an interpreter backed by `sdkClient`. `defaultRetryPolicy` is `None` by default —
   * matching the AWS SDK client's own zero-config behavior, which already retries on its own
   * (see `docs/reference/retries.md` for why running both layers at once is not recommended,
   * and the pairing to use instead). Any query that doesn't specify its own
   * `.withRetryPolicy(...)` falls back to `defaultRetryPolicy` when set, except `UpdateItem`,
   * which never retries without an explicit policy. `interceptors` bundles the three
   * independent, optional observability hooks (`ResponseInterceptor`/`RetryInterceptor`/
   * `BatchRetryInterceptor`); set only what you want, named:
   * `InterceptorConfig(retry = Some(myRetryInterceptor))`. `retryQuota` is a client-scoped
   * circuit breaker gating retries independent of any one call's own backoff curve, on by
   * default (`ZioRetryQuota.standard()`) since — unlike a retry policy — it can only ever
   * reduce retries below what one would otherwise allow, never add any; pass `None` to disable.
   */
  def fromAsyncClient(
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[Task] = InterceptorConfig(),
    defaultRetryPolicy: Option[EffectfulRetryPolicy[Task]] = None,
    retryQuota: Option[RetryQuota[Task]] = Some(ZioRetryQuota.standard())
  ): ZioInterpreter = {
    val client = buildClient(sdkClient, interceptors, plainBridge)
    new ZioInterpreter(client, defaultRetryPolicy, interceptors.retry, interceptors.batchRetry, retryQuota)
  }

  /**
   * Like [[fromAsyncClient]], but `.execute` on the returned interpreter fails with
   * [[DynamoFailure]] in ZIO's typed error channel. Existing code expecting `Task` still
   * compiles, but the failure values change: a raw SDK exception arrives wrapped (e.g.
   * `DynamoFailure.Rejected(e: ConditionalCheckFailedException)`).
   */
  def fromAsyncClientWithTypedErrors(
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[Task] = InterceptorConfig(),
    defaultRetryPolicy: Option[EffectfulRetryPolicy[Task]] = None,
    retryQuota: Option[RetryQuota[Task]] = Some(ZioRetryQuota.standard())
  ): TypedErrorsZioInterpreter = {
    val client        = buildClient(sdkClient, interceptors, classifyingBridge)
    val classifyAtRun =
      interceptors.response.nonEmpty || interceptors.retry.nonEmpty || interceptors.batchRetry.nonEmpty ||
        defaultRetryPolicy.nonEmpty
    new TypedErrorsZioInterpreter(
      client,
      defaultRetryPolicy,
      interceptors.retry,
      interceptors.batchRetry,
      retryQuota,
      classifyAtRun
    )
  }

  private def buildClient(
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[Task],
    fromFuture: FutureBridge
  ): AwsDynamoDB[Task] = {
    val base: AwsDynamoDB[Task] = new AwsDynamoDB[Task] {
      def getItem(req: GetItemRequest): Task[GetItemResponse]                                  =
        fromFuture(sdkClient.getItem(req))
      def putItem(req: PutItemRequest): Task[PutItemResponse]                                  =
        fromFuture(sdkClient.putItem(req))
      def updateItem(req: UpdateItemRequest): Task[UpdateItemResponse]                         =
        fromFuture(sdkClient.updateItem(req))
      def deleteItem(req: DeleteItemRequest): Task[DeleteItemResponse]                         =
        fromFuture(sdkClient.deleteItem(req))
      def batchGetItem(req: BatchGetItemRequest): Task[BatchGetItemResponse]                   =
        fromFuture(sdkClient.batchGetItem(req))
      def query(req: QueryRequest): Task[QueryResponse]                                        =
        fromFuture(sdkClient.query(req))
      def scan(req: ScanRequest): Task[ScanResponse]                                           =
        fromFuture(sdkClient.scan(req))
      def createTable(req: CreateTableRequest): Task[CreateTableResponse]                      =
        fromFuture(sdkClient.createTable(req))
      def deleteTable(req: DeleteTableRequest): Task[DeleteTableResponse]                      =
        fromFuture(sdkClient.deleteTable(req))
      def batchWriteItem(req: BatchWriteItemRequest): Task[BatchWriteItemResponse]             =
        fromFuture(sdkClient.batchWriteItem(req))
      def describeTable(req: DescribeTableRequest): Task[DescribeTableResponse]                =
        fromFuture(sdkClient.describeTable(req))
      def transactGetItems(req: TransactGetItemsRequest): Task[TransactGetItemsResponse]       =
        fromFuture(sdkClient.transactGetItems(req))
      def transactWriteItems(req: TransactWriteItemsRequest): Task[TransactWriteItemsResponse] =
        fromFuture(sdkClient.transactWriteItems(req))
    }
    val ops                     = new EffectOps[Task] {
      def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
      def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
    }
    interceptors.response.fold(base)(i => new InterceptingAwsDynamoDB[Task](base, i, ops))
  }
}
