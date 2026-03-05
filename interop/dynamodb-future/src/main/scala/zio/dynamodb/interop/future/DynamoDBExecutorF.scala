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

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import zio.aws.core.httpclient.Protocol

class DynamoDBExecutorF private (
  runtime: zio.Runtime.Scoped[DynamoDBExecutor],
  implicit val unsafe: Unsafe
) {
  def execute[A](query: DynamoDBQuery[_, A]): CancelableFuture[A] = {
    val zio: ZIO[DynamoDBExecutor, Throwable, A] =
      query.execute
    runtime.unsafe.runToFuture(zio)
  }

  def close(): Unit = runtime.shutdown0()
}
object DynamoDBExecutorF {
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
