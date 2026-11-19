---
id: dynamodb-executor
title: "DynamoDBExecutor"
description: "The ZIO service that runs DynamoDBQuery values against DynamoDB, how to build its layer and how to point it at DynamoDB Local."
keywords:
  - "DynamoDB"
  - "ZLayer"
  - "AWS client configuration"
  - "DynamoDBExecutor"
---

`DynamoDBExecutor` is the **ZIO service that runs queries**. A [`DynamoDBQuery`](dynamodb-query.md) is only a description of work; calling `DynamoDBQuery#execute` returns a `ZIO[DynamoDBExecutor, DynamoDBError, Out]`, and that effect does nothing until we provide a `DynamoDBExecutor` layer. The production layer, `DynamoDBExecutor.live`, translates each query into calls on the [ZIO AWS](https://zio.dev/zio-aws) DynamoDB client; the test layer, `DynamoDBExecutor.test`, keeps tables in memory.

The service has one method:

```scala
trait DynamoDBExecutor {
  def execute[A](query: DynamoDBQuery[_, A]): ZIO[Any, DynamoDBError, A]
}

object DynamoDBExecutor {
  val live: URLayer[DynamoDb, DynamoDBExecutor]
  lazy val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor]
  def test(tableAndPKNames: (String, String)*): ULayer[DynamoDBExecutor with TestDynamoDBExecutor]
}
```

You rarely call `DynamoDBExecutor#execute` directly; it is the low-level hook behind `DynamoDBQuery#execute`, which also does the batching and parallelisation described in [Auto Batching and Parallelisation](auto-batching-and-parallelisation.md). Prefer `query.execute` and provide the layer once at the edge of the program.

## Usage

To run a query against real AWS DynamoDB, we provide `DynamoDBExecutor.live` together with the ZIO AWS layers it depends on:

```scala mdoc:compile-only
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb._

object Main extends ZIOAppDefault {
  val program: ZIO[DynamoDBExecutor, DynamoDBError, Option[Item]] =
    DynamoDBQuery.getItem("person", PrimaryKey("id" -> "1")).execute

  override def run =
    program.provide(
      NettyHttpClient.default,
      AwsConfig.default,
      DynamoDb.live,
      DynamoDBExecutor.live
    )
}
```

## Building the Live Layer

`DynamoDBExecutor.live` needs a ZIO AWS `DynamoDb` client in the environment. That client in turn needs an `AwsConfig`, which needs an HTTP client. The layers stack like this:

```
NettyHttpClient.default   ZLayer[Any, Throwable, HttpClient]
        │
        ▼
AwsConfig.default         ZLayer[HttpClient, Nothing, AwsConfig]
        │
        ▼
DynamoDb.live             ZLayer[AwsConfig, Throwable, DynamoDb]
  or DynamoDb.customized(f)
        │
        ▼
DynamoDBExecutor.live     URLayer[DynamoDb, DynamoDBExecutor]
```

`AwsConfig.default` and `DynamoDb.live` take the region and credentials from the AWS SDK's default provider chains: the `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables, system properties, the `~/.aws` profile files, or the instance role. These layers come from the `zio-aws-core`, `zio-aws-netty` and `zio-aws-dynamodb` libraries, which `zio-dynamodb` already depends on.

We can wire the same stack once as a single layer and reuse it:

```scala mdoc:compile-only
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor

val dynamoDBExecutorLayer: ZLayer[Any, Throwable, DynamoDBExecutor] =
  NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.live >>> DynamoDBExecutor.live
```

## Customising the AWS Client

To change the endpoint, region or credentials of the underlying AWS SDK client, replace `DynamoDb.live` with `DynamoDb.customized`. It takes a function that adjusts the AWS SDK `DynamoDbAsyncClientBuilder`:

```scala
object DynamoDb {
  def customized(
    customization: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder
  ): ZLayer[AwsConfig, Throwable, DynamoDb]
}
```

The most common use is pointing the executor at [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html), which listens on `http://localhost:8000` and accepts any credentials:

```scala mdoc:compile-only
import java.net.URI

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor

val dynamoDbLocal: ZLayer[Any, Throwable, DynamoDb] =
  NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.customized { builder =>
    builder
      .endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
  }

val localExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] =
  dynamoDbLocal >>> DynamoDBExecutor.live
```

The repository ships a Docker Compose file that starts DynamoDB Local on port 8000 (`docker compose -f docker/docker-compose.yml up -d`). For a step-by-step setup, see [How to Connect to DynamoDB Local](../how-to/connect-to-dynamodb-local.md).

## How `DynamoDBQuery#execute` Uses the Executor

`DynamoDBQuery#execute` looks up the `DynamoDBExecutor` in the environment and hands it the query. Before it does, it splits a composite query (built with `zip`, `batch` and friends) into its parts and groups eligible `GetItem`, `PutItem` and `DeleteItem` operations into `BatchGetItem` and `BatchWriteItem` requests. The executor then:

- Converts each operation into a ZIO AWS request, including the expression attribute names and values.
- Retries unprocessed items of batch requests with the query's retry policy.
- Converts responses back into `Item` values, which the High Level API decodes with the codecs derived from the ZIO Schema in scope.
- Maps failures into `DynamoDBError`: AWS service exceptions become `DynamoDBError.AWSError`, other non-fatal exceptions become `DynamoDBError.UnknownNonFatalError`. See [Error Handling](error-handling.md).

## The Test Layer

`DynamoDBExecutor.test` and `DynamoDBExecutor.test(tableAndPKNames: _*)` provide an in-memory executor for unit tests. The layer also exposes the `TestDynamoDBExecutor` service, which we use to seed tables and inspect what ran. It supports a subset of operations; see [Test Executor](test-executor.md) for what it does and does not do.

To swap the live layer for the in-memory one in a test, we provide `DynamoDBExecutor.test` instead:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val program: ZIO[DynamoDBExecutor, DynamoDBError, Option[Item]] =
  DynamoDBQuery.getItem("person", PrimaryKey("id" -> "1")).execute

val inMemory: ZIO[Any, DynamoDBError, Option[Item]] =
  program.provide(DynamoDBExecutor.test("person" -> "id"))
```

## Interop Executors

The Cats Effect and `Future` interop modules wrap a `DynamoDBExecutor` so that non-ZIO code can run queries. They build the same live layer internally from an AWS SDK client builder function. See [How to Use ZIO DynamoDB From Cats Effect](../how-to/use-from-cats-effect.md) and [How to Use ZIO DynamoDB From Future-Based Code](../how-to/use-from-future-code.md).
