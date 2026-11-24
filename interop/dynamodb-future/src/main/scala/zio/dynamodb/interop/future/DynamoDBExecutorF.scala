package zio.dynamodb.interop.future

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.DynamoDBExecutor
import zio.ZIO

import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.aws.core.config

import zio.Unsafe
import zio.CancelableFuture
import zio.ZLayer

import software.amazon.awssdk.http.nio.netty.{ NettyNioAsyncHttpClient }
import zio.aws.core.httpclient.Protocol

/**
 * Runs a `DynamoDBQuery` as a `Future`. It owns a ZIO runtime that holds a `DynamoDBExecutor` and the AWS client behind
 * it. Create one with `DynamoDBExecutorF.make` and call `close` when it is no longer needed.
 */
class DynamoDBExecutorF private (
  runtime: zio.Runtime.Scoped[DynamoDBExecutor],
  implicit val unsafe: Unsafe
) {

  /** Runs `query` on the owned runtime and returns a `CancelableFuture` of its result. A failed query fails the future with a `DynamoDBError`. */
  def execute[A](query: DynamoDBQuery[_, A]): CancelableFuture[A] = {
    val zio: ZIO[DynamoDBExecutor, Throwable, A] =
      query.execute
    runtime.unsafe.runToFuture(zio)
  }

  /** Shuts the owned runtime down. */
  def close(): Unit = runtime.shutdown0()
}

/** Constructors of [[DynamoDBExecutorF]]. */
object DynamoDBExecutorF {

  /**
   * Builds the AWS client and the executor and starts the runtime that holds them, so it should be called once and the
   * result shared. `protocol` selects the HTTP protocol of the Netty client, `buildNettyClient` adjusts the Netty HTTP
   * client builder and `buildDynamoDbClient` adjusts the DynamoDB client builder.
   */
  def make(
    protocol: Protocol = Protocol.Http11,
    buildNettyClient: NettyNioAsyncHttpClient.Builder => NettyNioAsyncHttpClient.Builder = identity,
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): DynamoDBExecutorF = {

    val layer: ZLayer[Any, Throwable, zio.dynamodb.DynamoDBExecutor] =
      netty.NettyHttpClient.customized(
        protocol,
        buildNettyClient
      ) >+> config.AwsConfig.default >+> DynamoDb.customized(buildDynamoDbClient) >>> DynamoDBExecutor.live

    Unsafe.unsafe { implicit unsafe =>
      val runtime = zio.Runtime.unsafe.fromLayer(layer)

      new DynamoDBExecutorF(runtime, unsafe)
    }
  }

}
