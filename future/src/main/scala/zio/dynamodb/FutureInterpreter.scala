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
import scala.jdk.FutureConverters._
import scala.util.Success
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.DynamoDBError.ItemError

import java.util.concurrent.{ Executors, ScheduledExecutorService, TimeUnit }

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
class FutureInterpreter(client: AwsDynamoDB[Future])(implicit ec: ExecutionContext)
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

  /** Creates an interpreter backed by `sdkClient` with no interceptor. */
  def fromAsyncClient(sdkClient: DynamoDbAsyncClient)(implicit
    ec: ExecutionContext
  ): FutureInterpreter =
    fromAsyncClientInternal(sdkClient, None)

  /** Creates an interpreter that fires `interceptor` after every data operation. */
  def fromAsyncClient(
    sdkClient: DynamoDbAsyncClient,
    interceptor: ResponseInterceptor[Future]
  )(implicit ec: ExecutionContext): FutureInterpreter =
    fromAsyncClientInternal(sdkClient, Some(interceptor))

  private def fromAsyncClientInternal(
    sdkClient: DynamoDbAsyncClient,
    interceptor: Option[ResponseInterceptor[Future]]
  )(implicit ec: ExecutionContext): FutureInterpreter = {
    val base: AwsDynamoDB[Future]   = new AwsDynamoDB[Future] {
      def getItem(req: GetItemRequest): Future[GetItemResponse]                                  = sdkClient.getItem(req).asScala
      def putItem(req: PutItemRequest): Future[PutItemResponse]                                  = sdkClient.putItem(req).asScala
      def updateItem(req: UpdateItemRequest): Future[UpdateItemResponse]                         = sdkClient.updateItem(req).asScala
      def deleteItem(req: DeleteItemRequest): Future[DeleteItemResponse]                         = sdkClient.deleteItem(req).asScala
      def batchGetItem(req: BatchGetItemRequest): Future[BatchGetItemResponse]                   = sdkClient.batchGetItem(req).asScala
      def batchWriteItem(req: BatchWriteItemRequest): Future[BatchWriteItemResponse]             =
        sdkClient.batchWriteItem(req).asScala
      def query(req: QueryRequest): Future[QueryResponse]                                        = sdkClient.query(req).asScala
      def scan(req: ScanRequest): Future[ScanResponse]                                           = sdkClient.scan(req).asScala
      def createTable(req: CreateTableRequest): Future[CreateTableResponse]                      = sdkClient.createTable(req).asScala
      def deleteTable(req: DeleteTableRequest): Future[DeleteTableResponse]                      = sdkClient.deleteTable(req).asScala
      def describeTable(req: DescribeTableRequest): Future[DescribeTableResponse]                = sdkClient.describeTable(req).asScala
      def transactGetItems(req: TransactGetItemsRequest): Future[TransactGetItemsResponse]       =
        sdkClient.transactGetItems(req).asScala
      def transactWriteItems(req: TransactWriteItemsRequest): Future[TransactWriteItemsResponse] =
        sdkClient.transactWriteItems(req).asScala
    }
    val ops                         = new EffectOps[Future] {
      def map[A, B](fa: Future[A])(f: A => B): Future[B]             = fa.map(f)
      def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    }
    val client: AwsDynamoDB[Future] =
      interceptor.fold(base)(i => new InterceptingAwsDynamoDB[Future](base, i, ops))
    new FutureInterpreter(client)
  }
}
