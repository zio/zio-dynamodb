package zio.dynamodb.interop.future

import zio.dynamodb.DynamoDBQuery
import zio.dynamodb.DynamoDBExecutor
import zio.ZIO

//import scala.concurrent.duration._
//import scala.concurrent.{ ExecutionContext, Future }

import zio.aws.dynamodb.DynamoDb
import zio.aws.netty
import zio.aws.core.config

import zio.Unsafe
import zio.CancelableFuture
import zio.ZLayer

import software.amazon.awssdk.http.nio.netty.{ NettyNioAsyncHttpClient }

/*
Create a DynamoDBExecutorF with make, with a close method to release resources.

for {
  _ <- DynamoDbQuery.put(...).executeToF(ddbExecutor)
  _ <- DynamoDbQuery.put(...).executeToF // implicit
} yield ???



 */

class DynamoDBExecutorF(
  runtime: zio.Runtime.Scoped[DynamoDBExecutor],
  implicit val unsafe: Unsafe
)                        {
  def execute[A](query: DynamoDBQuery[_, A]): CancelableFuture[A] = {
    val zio: ZIO[DynamoDBExecutor, Throwable, A] =
      query.execute
    runtime.unsafe.runToFuture(zio)
  }
}
object DynamoDBExecutorF {
  def make(
    // bunch of builder here
    //buildKinesisClient: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity,
    buildNettyClient: NettyNioAsyncHttpClient.Builder => NettyNioAsyncHttpClient.Builder = identity
  ): DynamoDBExecutorF = {
    println(buildNettyClient)

    // build the layers for the DynamoDBExecutor
    // create the runtime from the layers
    /*
    program.provide(
      netty.NettyHttpClient.default,
      config.AwsConfig.default, // uses real AWS dynamodb
      dynamodb.DynamoDb.live,
      DynamoDBExecutor.live
    )
     */
    val layer: ZLayer[Any, Throwable, zio.dynamodb.DynamoDBExecutor] =
      netty.NettyHttpClient.default >+> config.AwsConfig.default >+> DynamoDb.live >>> DynamoDBExecutor.live

    // val runtime: Runtime.Scoped[zio.dynamodb.DynamoDBExecutor] =
    //   zio.Runtime.fromLayer(layer) // or any other runtime you want to use
    //new DynamoDBExecutorF(runtime, Unsafe.unsafe(implicit u => u))
    Unsafe.unsafe { implicit unsafe =>
      val runtime = zio.Runtime.unsafe.fromLayer(layer)

      new DynamoDBExecutorF(runtime, unsafe)
    }
  }

  def close(executor: DynamoDBExecutorF): Unit = {
    // Logic to close resources if needed
  }
}

/*
object Consumer {
  def make(
    buildKinesisClient: KinesisAsyncClientBuilder => KinesisAsyncClientBuilder = identity,
    buildCloudWatchClient: CloudWatchAsyncClientBuilder => CloudWatchAsyncClientBuilder = identity,
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity,
    buildHttpClient: NettyNioAsyncHttpClient.Builder => SdkAsyncHttpClient = _.build()
  ): Consumer = {

    val sdkClients = HttpClientBuilder.make(build = buildHttpClient) >>> config.AwsConfig.default >>> (
      kinesisAsyncClientLayer(buildKinesisClient) ++
        cloudWatchAsyncClientLayer(buildCloudWatchClient) ++
        dynamoDbAsyncClientLayer(buildDynamoDbClient)
    )

    val layer = (sdkClients >+> DynamoDbLeaseRepository.live)

    Unsafe.unsafe { implicit unsafe =>
      val runtime = zio.Runtime.unsafe.fromLayer(layer)

      new Consumer(runtime, unsafe)
    }
  }
}

 */
