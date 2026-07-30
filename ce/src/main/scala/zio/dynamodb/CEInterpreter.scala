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

import cats.effect.{ IO, IOLocal }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.DynamoDBError.ItemError

import scala.concurrent.duration.FiniteDuration

class CEInterpreter(client: AwsDynamoDB[IO]) extends RealAwsInterpreter[IO](client) {
  private[dynamodb] def pure[A](a: A): IO[A]                           = IO.pure(a)
  private[dynamodb] def map[A, B](fa: IO[A])(f: A => B): IO[B]         = fa.map(f)
  private[dynamodb] def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
  protected def product[A, B](fa: IO[A], fb: IO[B]): IO[(A, B)]        =
    fa.flatMap(a => fb.map(b => (a, b)))
  protected def productPar[A, B](fa: IO[A], fb: IO[B]): IO[(A, B)]     =
    IO.both(fa, fb)
  protected def fail[A](e: DynamoDBError): IO[A]                       = IO.raiseError(e)
  protected def absolve[A](fa: IO[Either[ItemError, A]]): IO[A]        =
    fa.flatMap {
      case Right(a) => IO.pure(a)
      case Left(e)  => IO.raiseError(e)
    }

  private[dynamodb] def sleep(d: FiniteDuration): IO[Unit]              =
    IO.sleep(d)
  private[dynamodb] def attempt[A](fa: IO[A]): IO[Either[Throwable, A]] =
    fa.attempt
  private[dynamodb] def raiseError[A](t: Throwable): IO[A]              =
    IO.raiseError(t)
}

object CEInterpreter {

  /** Creates an interpreter backed by `sdkClient` with no interceptor. */
  def fromAsyncClient(sdkClient: DynamoDbAsyncClient): CEInterpreter =
    fromAsyncClientInternal(sdkClient, None)

  /** Creates an interpreter that fires `interceptor` after every data operation. */
  def fromAsyncClient(
    sdkClient: DynamoDbAsyncClient,
    interceptor: ResponseInterceptor[IO]
  ): CEInterpreter =
    fromAsyncClientInternal(sdkClient, Some(interceptor))

  private def fromAsyncClientInternal(
    sdkClient: DynamoDbAsyncClient,
    interceptor: Option[ResponseInterceptor[IO]]
  ): CEInterpreter = {
    val base: AwsDynamoDB[IO]   = new AwsDynamoDB[IO] {
      def getItem(req: GetItemRequest): IO[GetItemResponse]                                  =
        IO.fromCompletableFuture(IO(sdkClient.getItem(req)))
      def putItem(req: PutItemRequest): IO[PutItemResponse]                                  =
        IO.fromCompletableFuture(IO(sdkClient.putItem(req)))
      def updateItem(req: UpdateItemRequest): IO[UpdateItemResponse]                         =
        IO.fromCompletableFuture(IO(sdkClient.updateItem(req)))
      def deleteItem(req: DeleteItemRequest): IO[DeleteItemResponse]                         =
        IO.fromCompletableFuture(IO(sdkClient.deleteItem(req)))
      def batchGetItem(req: BatchGetItemRequest): IO[BatchGetItemResponse]                   =
        IO.fromCompletableFuture(IO(sdkClient.batchGetItem(req)))
      def querySome(req: QueryRequest): IO[QueryResponse]                                    =
        IO.fromCompletableFuture(IO(sdkClient.query(req)))
      def scanSome(req: ScanRequest): IO[ScanResponse]                                       =
        IO.fromCompletableFuture(IO(sdkClient.scan(req)))
      def createTable(req: CreateTableRequest): IO[CreateTableResponse]                      =
        IO.fromCompletableFuture(IO(sdkClient.createTable(req)))
      def deleteTable(req: DeleteTableRequest): IO[DeleteTableResponse]                      =
        IO.fromCompletableFuture(IO(sdkClient.deleteTable(req)))
      def batchWriteItem(req: BatchWriteItemRequest): IO[BatchWriteItemResponse]             =
        IO.fromCompletableFuture(IO(sdkClient.batchWriteItem(req)))
      def describeTable(req: DescribeTableRequest): IO[DescribeTableResponse]                =
        IO.fromCompletableFuture(IO(sdkClient.describeTable(req)))
      def transactGetItems(req: TransactGetItemsRequest): IO[TransactGetItemsResponse]       =
        IO.fromCompletableFuture(IO(sdkClient.transactGetItems(req)))
      def transactWriteItems(req: TransactWriteItemsRequest): IO[TransactWriteItemsResponse] =
        IO.fromCompletableFuture(IO(sdkClient.transactWriteItems(req)))
    }
    val ops                     = new EffectOps[IO] {
      def map[A, B](fa: IO[A])(f: A => B): IO[B]         = fa.map(f)
      def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
    }
    val client: AwsDynamoDB[IO] =
      interceptor.fold(base)(i => new InterceptingAwsDynamoDB[IO](base, i, ops))
    new CEInterpreter(client)
  }
}
