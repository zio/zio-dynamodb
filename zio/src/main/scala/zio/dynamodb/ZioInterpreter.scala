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

import scala.concurrent.duration.FiniteDuration

class ZioInterpreter(client: AwsDynamoDB[Task]) extends RealAwsInterpreter[Task](client) {
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

object ZioInterpreter {

  /** Creates an interpreter backed by `sdkClient` with no interceptor. */
  def fromAsyncClient(sdkClient: DynamoDbAsyncClient): ZioInterpreter =
    fromAsyncClientInternal(sdkClient, None)

  /** Creates an interpreter that fires `interceptor` after every data operation. */
  def fromAsyncClient(
    sdkClient: DynamoDbAsyncClient,
    interceptor: ResponseInterceptor[Task]
  ): ZioInterpreter =
    fromAsyncClientInternal(sdkClient, Some(interceptor))

  private def fromAsyncClientInternal(
    sdkClient: DynamoDbAsyncClient,
    interceptor: Option[ResponseInterceptor[Task]]
  ): ZioInterpreter = {
    val base: AwsDynamoDB[Task]   = new AwsDynamoDB[Task] {
      def getItem(req: GetItemRequest): Task[GetItemResponse]                                  =
        ZIO.fromCompletableFuture(sdkClient.getItem(req))
      def putItem(req: PutItemRequest): Task[PutItemResponse]                                  =
        ZIO.fromCompletableFuture(sdkClient.putItem(req))
      def updateItem(req: UpdateItemRequest): Task[UpdateItemResponse]                         =
        ZIO.fromCompletableFuture(sdkClient.updateItem(req))
      def deleteItem(req: DeleteItemRequest): Task[DeleteItemResponse]                         =
        ZIO.fromCompletableFuture(sdkClient.deleteItem(req))
      def batchGetItem(req: BatchGetItemRequest): Task[BatchGetItemResponse]                   =
        ZIO.fromCompletableFuture(sdkClient.batchGetItem(req))
      def querySome(req: QueryRequest): Task[QueryResponse]                                    =
        ZIO.fromCompletableFuture(sdkClient.query(req))
      def scanSome(req: ScanRequest): Task[ScanResponse]                                       =
        ZIO.fromCompletableFuture(sdkClient.scan(req))
      def createTable(req: CreateTableRequest): Task[CreateTableResponse]                      =
        ZIO.fromCompletableFuture(sdkClient.createTable(req))
      def deleteTable(req: DeleteTableRequest): Task[DeleteTableResponse]                      =
        ZIO.fromCompletableFuture(sdkClient.deleteTable(req))
      def batchWriteItem(req: BatchWriteItemRequest): Task[BatchWriteItemResponse]             =
        ZIO.fromCompletableFuture(sdkClient.batchWriteItem(req))
      def describeTable(req: DescribeTableRequest): Task[DescribeTableResponse]                =
        ZIO.fromCompletableFuture(sdkClient.describeTable(req))
      def transactGetItems(req: TransactGetItemsRequest): Task[TransactGetItemsResponse]       =
        ZIO.fromCompletableFuture(sdkClient.transactGetItems(req))
      def transactWriteItems(req: TransactWriteItemsRequest): Task[TransactWriteItemsResponse] =
        ZIO.fromCompletableFuture(sdkClient.transactWriteItems(req))
    }
    val ops                       = new EffectOps[Task] {
      def map[A, B](fa: Task[A])(f: A => B): Task[B]           = fa.map(f)
      def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
    }
    val client: AwsDynamoDB[Task] =
      interceptor.fold(base)(i => new InterceptingAwsDynamoDB[Task](base, i, ops))
    new ZioInterpreter(client)
  }
}
