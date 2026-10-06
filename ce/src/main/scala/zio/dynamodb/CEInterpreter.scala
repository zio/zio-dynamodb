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

import cats.effect.Async
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.dynamodb.DynamoDBError.ItemError

import scala.concurrent.duration.FiniteDuration

/**
 * Generic over any `F[_]: Async`, not just `cats.effect.IO` — every primitive below is backed
 * by `Async[F]`'s own typeclass methods (itself extending `Sync`/`Temporal`/`Concurrent`),
 * which `IO` has an instance for like any other `Async`-conformant effect type. Construct via
 * [[CEInterpreter.fromAsyncClient]].
 */
class CEInterpreter[F[_]](
  client: AwsDynamoDB[F],
  override protected val defaultRetryPolicy: Option[EffectfulRetryPolicy[F]] = None,
  override protected val retryInterceptor: Option[RetryInterceptor[F]] = None,
  override protected val batchRetryInterceptor: Option[BatchRetryInterceptor[F]] = None,
  override protected val retryQuota: Option[RetryQuota[F]] = None
)(implicit F: Async[F])
    extends RealAwsInterpreter[F](client) {
  private[dynamodb] def pure[A](a: A): F[A]                         = F.pure(a)
  private[dynamodb] def map[A, B](fa: F[A])(f: A => B): F[B]        = F.map(fa)(f)
  private[dynamodb] def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] = F.flatMap(fa)(f)
  protected def product[A, B](fa: F[A], fb: F[B]): F[(A, B)]        =
    F.flatMap(fa)(a => F.map(fb)(b => (a, b)))
  protected def productPar[A, B](fa: F[A], fb: F[B]): F[(A, B)]     =
    F.both(fa, fb)
  protected def fail[A](e: DynamoDBError): F[A]                     = F.raiseError(e)
  protected def absolve[A](fa: F[Either[ItemError, A]]): F[A]       =
    F.flatMap(fa) {
      case Right(a) => F.pure(a)
      case Left(e)  => F.raiseError(e)
    }

  private[dynamodb] def sleep(d: FiniteDuration): F[Unit]             = F.sleep(d)
  private[dynamodb] def attempt[A](fa: F[A]): F[Either[Throwable, A]] = F.attempt(fa)
  private[dynamodb] def raiseError[A](t: Throwable): F[A]             = F.raiseError(t)
}

object CEInterpreter {

  /**
   * Creates an interpreter backed by `sdkClient`, generic over any `F[_]: Async` — pick
   * `F = cats.effect.IO` for the common case, or your own `Async`-conformant effect type.
   * `defaultRetryPolicy` is `None` by default — matching the AWS SDK client's own zero-config
   * behavior, which already retries on its own (see `docs/reference/retries.md` for why running
   * both layers at once is not recommended, and the pairing to use instead). Any query that
   * doesn't specify its own `.withRetryPolicy(...)` falls back to `defaultRetryPolicy` when set,
   * except `UpdateItem`, which never retries without an explicit policy. `interceptors` bundles
   * the three independent, optional observability hooks (`ResponseInterceptor`/
   * `RetryInterceptor`/`BatchRetryInterceptor`); set only what you want, named:
   * `InterceptorConfig(retry = Some(myRetryInterceptor))`. `retryQuota` is a client-scoped
   * circuit breaker gating retries independent of any one call's own backoff curve, on by
   * default (`CERetryQuota.standard()`) since — unlike a retry policy — it can only ever
   * reduce retries below what one would otherwise allow, never add any; pass `None` to disable.
   */
  def fromAsyncClient[F[_]: Async](
    sdkClient: DynamoDbAsyncClient,
    interceptors: InterceptorConfig[F] = null,
    defaultRetryPolicy: Option[EffectfulRetryPolicy[F]] = None,
    retryQuota: Option[RetryQuota[F]] = null
  ): CEInterpreter[F] =
    fromAsyncClientInternal(sdkClient, Option(interceptors), defaultRetryPolicy, Option(retryQuota))

  // `interceptors`/`retryQuota`'s defaults can't be literal default-argument expressions above:
  // default-argument expressions are compiled as separate synthetic methods that neither
  // inherit the declaring method's own type parameter from the call site nor its context-bound
  // evidence (here, Async[F]) — so `InterceptorConfig()` can't even infer F, and
  // `CERetryQuota.standard[F]()` has no Async[F] to call. Using `null` as the sentinel default
  // for both and resolving their real defaults here, where F is pinned and F: Async is an
  // ordinary (non-default-argument) implicit, sidesteps both without changing the public call
  // shape or either default's behavior.
  private def fromAsyncClientInternal[F[_]](
    sdkClient: DynamoDbAsyncClient,
    interceptors: Option[InterceptorConfig[F]],
    defaultRetryPolicy: Option[EffectfulRetryPolicy[F]],
    retryQuota: Option[Option[RetryQuota[F]]]
  )(implicit F: Async[F]): CEInterpreter[F] = {
    val resolvedInterceptors: InterceptorConfig[F] = interceptors.getOrElse(InterceptorConfig())
    val resolvedRetryQuota: Option[RetryQuota[F]]  = retryQuota.getOrElse(Some(CERetryQuota.standard[F]()))
    val base: AwsDynamoDB[F]                       = new AwsDynamoDB[F] {
      def getItem(req: GetItemRequest): F[GetItemResponse]                                  =
        F.fromCompletableFuture(F.delay(sdkClient.getItem(req)))
      def putItem(req: PutItemRequest): F[PutItemResponse]                                  =
        F.fromCompletableFuture(F.delay(sdkClient.putItem(req)))
      def updateItem(req: UpdateItemRequest): F[UpdateItemResponse]                         =
        F.fromCompletableFuture(F.delay(sdkClient.updateItem(req)))
      def deleteItem(req: DeleteItemRequest): F[DeleteItemResponse]                         =
        F.fromCompletableFuture(F.delay(sdkClient.deleteItem(req)))
      def batchGetItem(req: BatchGetItemRequest): F[BatchGetItemResponse]                   =
        F.fromCompletableFuture(F.delay(sdkClient.batchGetItem(req)))
      def query(req: QueryRequest): F[QueryResponse]                                        =
        F.fromCompletableFuture(F.delay(sdkClient.query(req)))
      def scan(req: ScanRequest): F[ScanResponse]                                           =
        F.fromCompletableFuture(F.delay(sdkClient.scan(req)))
      def createTable(req: CreateTableRequest): F[CreateTableResponse]                      =
        F.fromCompletableFuture(F.delay(sdkClient.createTable(req)))
      def deleteTable(req: DeleteTableRequest): F[DeleteTableResponse]                      =
        F.fromCompletableFuture(F.delay(sdkClient.deleteTable(req)))
      def batchWriteItem(req: BatchWriteItemRequest): F[BatchWriteItemResponse]             =
        F.fromCompletableFuture(F.delay(sdkClient.batchWriteItem(req)))
      def describeTable(req: DescribeTableRequest): F[DescribeTableResponse]                =
        F.fromCompletableFuture(F.delay(sdkClient.describeTable(req)))
      def transactGetItems(req: TransactGetItemsRequest): F[TransactGetItemsResponse]       =
        F.fromCompletableFuture(F.delay(sdkClient.transactGetItems(req)))
      def transactWriteItems(req: TransactWriteItemsRequest): F[TransactWriteItemsResponse] =
        F.fromCompletableFuture(F.delay(sdkClient.transactWriteItems(req)))
    }
    val ops                                        = new EffectOps[F] {
      def map[A, B](fa: F[A])(f: A => B): F[B]        = F.map(fa)(f)
      def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] = F.flatMap(fa)(f)
    }
    val client: AwsDynamoDB[F]                     =
      resolvedInterceptors.response.fold(base)(i => new InterceptingAwsDynamoDB[F](base, i, ops))
    new CEInterpreter[F](
      client,
      defaultRetryPolicy,
      resolvedInterceptors.retry,
      resolvedInterceptors.batchRetry,
      resolvedRetryQuota
    )
  }
}
