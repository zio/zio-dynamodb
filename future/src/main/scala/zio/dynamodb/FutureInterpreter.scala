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

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.DynamoDBError.ItemError

import java.util.concurrent.{ CompletableFuture, CompletionException, Executors, ScheduledExecutorService, TimeUnit }

/**
 * DynamoDB interpreter backed by [[scala.concurrent.Future]].
 *  Internally converts the AWS SDK async client's `java.util.concurrent.CompletableFuture`
 *  results to Scala Futures. Requires no effect-system dependency beyond the Scala standard library.
 *
 *  Construct via [[FutureInterpreter.fromAsyncClient]].
 *
 *  @param client an [[AwsDynamoDB]] implementation (may be plain or intercepting)
 *  @param ec     the execution context used to sequence `map`/`flatMap` callbacks
 */
class FutureInterpreter(
  client: AwsDynamoDB[Future],
  override protected val defaultRetryPolicy: Option[EffectfulRetryPolicy[Future]] = None,
  override protected val retryInterceptor: Option[RetryInterceptor[Future]] = None,
  override protected val batchUnprocessedRetryInterceptor: Option[BatchUnprocessedRetryInterceptor[Future]] = None,
  override protected val retryQuota: Option[RetryQuota[Future]] = None,
  override protected val defaultBatchUnprocessedRetryPolicy: Option[RetryPolicy] = None
)(implicit ec: ExecutionContext)
    extends RealAwsInterpreter[Future](client) {

  private[dynamodb] def pure[A](a: A): Future[A]                                   = Future.successful(a)
  private[dynamodb] def map[A, B](fa: Future[A])(f: A => B): Future[B]             = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
    fa.flatMap(f)
  protected def product[A, B](fa: Future[A], fb: Future[B]): Future[(A, B)]        = fa.zip(fb)
  protected def productPar[A, B](fa: Future[A], fb: Future[B]): Future[(A, B)]     = fa.zip(fb)
  protected def fail[A](e: DynamoDBError): Future[A]                               = Future.failed(e)
  protected def absolve[A](fa: Future[Either[ItemError, A]]): Future[A]            =
    fa.flatMap {
      case Right(a) => Future.successful(a)
      case Left(e)  => Future.failed(e)
    }

  // Uses a shared daemon ScheduledExecutorService — does not block a thread.
  private[dynamodb] def sleep(d: FiniteDuration): Future[Unit] = {
    val p = Promise[Unit]()
    FutureInterpreter.scheduler.schedule(
      new Runnable { def run(): Unit = { p.success(()); () } },
      d.toMillis,
      TimeUnit.MILLISECONDS
    )
    p.future
  }

  private[dynamodb] def attempt[A](fa: Future[A]): Future[Either[Throwable, A]] =
    fa.transform(t => Success(t.toEither))

  private[dynamodb] def raiseError[A](t: Throwable): Future[A] =
    Future.failed(t)
}

object FutureInterpreter {

  private[dynamodb] val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r =>
      val t = new Thread(r, "zio-dynamodb-future-scheduler")
      t.setDaemon(true)
      t
    }

  /**
   * Builds an interpreter on `sdkClient`.
   *
   * @param interceptors optional response, retry and batch-unprocessed hooks; set only those you
   * need, e.g. `InterceptorConfig(retry = Some(myRetryInterceptor))`.
   * @param defaultRetryPolicy the retry policy for calls that don't set `.withRetryPolicy`. Off by
   * default, because the AWS SDK client already retries; `docs/reference/retries.md` describes the
   * pairing to use instead. Never applied to `updateItem`.
   * @param retryQuota a client-wide limit on retries, on by default; `None` turns it off. It can only
   * reduce retries, never add any.
   * @param defaultBatchUnprocessedRetryPolicy resubmits a batch's unprocessed keys / items for calls
   * that don't set `.withBatchUnprocessedRetryPolicy`. On by default; `None` returns `Incomplete`
   * instead. Independent of `defaultRetryPolicy`.
   */
  def fromAsyncClient(
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[Future] = InterceptorConfig(),
    defaultRetryPolicy: Option[EffectfulRetryPolicy[Future]] = None,
    retryQuota: Option[RetryQuota[Future]] = Some(FutureRetryQuota.standard()),
    defaultBatchUnprocessedRetryPolicy: Option[RetryPolicy] = Some(RetryPolicy.batchUnprocessedDefault)
  )(implicit ec: ExecutionContext): FutureInterpreter =
    fromAsyncClientInternal(sdkClient, interceptors, defaultRetryPolicy, retryQuota, defaultBatchUnprocessedRetryPolicy)

  // The SDK's futures usually fail with a CompletionException around the real exception. Unwrap it
  // so callers - and the library's own error translation and retry classification - see the SDK
  // exception itself, as they do with the other interpreters.
  private def toScala[A](cf: CompletableFuture[A]): Future[A] = {
    val p = Promise[A]()
    cf.whenComplete { (a: A, t: Throwable) =>
      if (t eq null) p.success(a)
      else
        p.failure(t match {
          case c: CompletionException if c.getCause ne null => c.getCause
          case other                                        => other
        })
      ()
    }
    p.future
  }

  private def fromAsyncClientInternal(
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[Future],
    defaultRetryPolicy: Option[EffectfulRetryPolicy[Future]],
    retryQuota: Option[RetryQuota[Future]],
    defaultBatchUnprocessedRetryPolicy: Option[RetryPolicy]
  )(implicit ec: ExecutionContext): FutureInterpreter = {
    val base: AwsDynamoDB[Future]   = new AwsDynamoDB[Future] {
      def getItem(req: GetItemRequest): Future[GetItemResponse]                                  = toScala(sdkClient.getItem(req))
      def putItem(req: PutItemRequest): Future[PutItemResponse]                                  = toScala(sdkClient.putItem(req))
      def updateItem(req: UpdateItemRequest): Future[UpdateItemResponse]                         = toScala(sdkClient.updateItem(req))
      def deleteItem(req: DeleteItemRequest): Future[DeleteItemResponse]                         = toScala(sdkClient.deleteItem(req))
      def batchGetItem(req: BatchGetItemRequest): Future[BatchGetItemResponse]                   = toScala(sdkClient.batchGetItem(req))
      def batchWriteItem(req: BatchWriteItemRequest): Future[BatchWriteItemResponse]             =
        toScala(sdkClient.batchWriteItem(req))
      def query(req: QueryRequest): Future[QueryResponse]                                        = toScala(sdkClient.query(req))
      def scan(req: ScanRequest): Future[ScanResponse]                                           = toScala(sdkClient.scan(req))
      def createTable(req: CreateTableRequest): Future[CreateTableResponse]                      = toScala(sdkClient.createTable(req))
      def deleteTable(req: DeleteTableRequest): Future[DeleteTableResponse]                      = toScala(sdkClient.deleteTable(req))
      def describeTable(req: DescribeTableRequest): Future[DescribeTableResponse]                = toScala(
        sdkClient.describeTable(req)
      )
      def transactGetItems(req: TransactGetItemsRequest): Future[TransactGetItemsResponse]       =
        toScala(sdkClient.transactGetItems(req))
      def transactWriteItems(req: TransactWriteItemsRequest): Future[TransactWriteItemsResponse] =
        toScala(sdkClient.transactWriteItems(req))
    }
    val ops                         = new EffectOps[Future] {
      def map[A, B](fa: Future[A])(f: A => B): Future[B]             = fa.map(f)
      def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    }
    val client: AwsDynamoDB[Future] =
      interceptors.response.fold(base)(i => new InterceptingAwsDynamoDB[Future](base, i, ops))
    new FutureInterpreter(
      client,
      defaultRetryPolicy,
      interceptors.retry,
      interceptors.batchUnprocessedRetry,
      retryQuota,
      defaultBatchUnprocessedRetryPolicy
    )
  }
}
