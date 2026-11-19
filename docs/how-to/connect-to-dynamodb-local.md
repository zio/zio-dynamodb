---
id: connect-to-dynamodb-local
title: "How to Connect to DynamoDB Local or Customise the AWS Client"
sidebar_label: "Connect to DynamoDB Local"
description: "Build the DynamoDBExecutor layer for DynamoDB Local or real AWS, and customise the endpoint, region, credentials and HTTP client."
keywords:
  - "DynamoDB Local"
  - "AWS Client Configuration"
  - "ZLayer"
  - "DynamoDBExecutor"
  - "endpointOverride"
---

In this guide, we point a ZIO DynamoDB program at DynamoDB Local, the database AWS ships for development and testing, and then at real AWS. By the end, you will have a `DynamoDBExecutor` layer for each environment, a layer that creates and drops a table around a run, and the places where you can tune the AWS SDK client.

We start DynamoDB Local in Docker, build a local layer, run a program against it, add table setup, switch to real AWS, and finish with client customisation.

## The Problem

Every query runs through a `DynamoDBExecutor`, and `DynamoDBExecutor.live` needs a `zio.aws.dynamodb.DynamoDb` client. That client in turn needs an AWS configuration and an HTTP client. Out of the box the AWS SDK sends requests to the real AWS endpoint of the region it finds in your environment, signed with the credentials it finds there.

On a developer laptop or in CI that is rarely what you want: the tests would touch a real account, cost money and fail without credentials. DynamoDB Local listens on `http://localhost:8000` and accepts any credentials, but the SDK has to be told to use that endpoint, given a region and given some credentials to sign with. All of this happens while building the layer, so the rest of the program stays the same in every environment.

## Prerequisites

We need the core module, which brings in the ZIO AWS DynamoDB client and its Netty HTTP client:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The local examples also need Docker. The examples share this model of a customer stored in a table whose partition key is `id`:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Customer(id: String, name: String)
object Customer {
  implicit val schema: Schema.CaseClass2[String, String, Customer] = DeriveSchema.gen[Customer]

  val (id, name) = ProjectionExpression.accessors[Customer]
}
```

## Starting DynamoDB Local

AWS publishes DynamoDB Local as the `amazon/dynamodb-local` Docker image. The ZIO DynamoDB repository runs it for its own integration tests with this `docker/docker-compose.yml`:

```yaml
version: '3.7'
services:
  dynamodb:
    image:  amazon/dynamodb-local
    container_name: dynamodb-local
    hostname: dynamodb
    restart: always
    ports:
      - 8000:8000
    command: "-jar DynamoDBLocal.jar -inMemory -sharedDb"
```

To start the same database without a compose file:

```bash
docker run --rm -p 8000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory -sharedDb
```

`-inMemory` keeps the data in memory, so every restart begins with an empty database, and `-sharedDb` makes all credentials and regions see the same tables.

## Building a Layer for DynamoDB Local

`zio.aws.dynamodb.DynamoDb.customized` takes a function that adjusts the AWS SDK's `DynamoDbAsyncClientBuilder`. We use it to override the endpoint, fix the region and supply dummy static credentials, then feed the result to `DynamoDBExecutor.live`.

To build the local executor layer:

```scala mdoc:silent
import java.net.URI
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient

val localDynamoDb: ZLayer[Any, Throwable, DynamoDb] =
  NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.customized { builder =>
    builder
      .endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
  }

val localExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] =
  localDynamoDb >>> DynamoDBExecutor.live
```

The three layers form a chain: `NettyHttpClient.default` provides the HTTP client, `AwsConfig.default` wraps it in an AWS configuration, and `DynamoDb.customized` builds the client from that configuration plus our adjustments.

## Running a Program Against It

The program itself only needs a `DynamoDBExecutor`; the layer decides where it runs.

To create a table, write and read a customer, and drop the table, all against DynamoDB Local:

```scala mdoc:compile-only
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable, get, put }

object LocalApp extends ZIOAppDefault {
  val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
    for {
      _        <- createTable("customers", KeySchema("id"), BillingMode.PayPerRequest)(
                    AttributeDefinition.attrDefnString("id")
                  ).execute
      _        <- put("customers", Customer("c-1", "Ada")).execute
      customer <- get("customers")(Customer.id.partitionKey === "c-1").execute.absolve
      _        <- ZIO.debug(s"found $customer")
      _        <- deleteTable("customers").execute
    } yield ()

  def run = program.provide(localExecutor)
}
```

`ZIO#absolve` moves the `Either` returned by `DynamoDBQuery.get` into the error channel; see [get](../reference/high-level-api/crud-operations/get.md) for other ways to handle a missing item.

## Creating and Dropping a Table Around a Run

Integration tests usually want a fresh table per run. A scoped layer can create the table when it is built and drop it when the scope closes, the same way the repository's `DynamoDBLocalMain` example does.

To define a table layer that depends on the executor:

```scala mdoc:silent
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable }

val customersTable: ZLayer[DynamoDBExecutor, Throwable, Unit] =
  ZLayer.scoped(
    ZIO.acquireRelease(
      createTable("customers", KeySchema("id"), BillingMode.PayPerRequest)(
        AttributeDefinition.attrDefnString("id")
      ).execute
    )(_ => deleteTable("customers").execute.orDie)
  )
```

A program then asks for both, for example `program.provide(localExecutor, customersTable)`, and the table exists exactly as long as the program runs.

## Connecting to Real AWS

For real AWS, drop the customisation. `DynamoDb.live` uses the AWS SDK defaults: the default credentials provider chain (environment variables, Java system properties, the `~/.aws` profile files, then container or instance roles) and the default region provider chain (for example the `AWS_REGION` environment variable or the profile's region).

To build the production executor layer:

```scala mdoc:silent
val awsExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] =
  NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.live >>> DynamoDBExecutor.live
```

Because the same SDK chains run locally too, an alternative to the static credentials above is to export `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION` with dummy values and keep only the `endpointOverride` in the builder. The ZIO DynamoDB build does exactly that for its own integration tests.

## Customising the AWS Client

The builder function passed to `DynamoDb.customized` receives the full AWS SDK builder, so any SDK setting is available there, such as timeouts through `overrideConfiguration`. The HTTP client is configured separately with `NettyHttpClient.customized`, which takes the protocol and a function over the Netty builder.

To set an API call timeout and cap the number of concurrent connections:

```scala mdoc:silent
import java.time.Duration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import zio.aws.core.httpclient.Protocol

val tunedExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] =
  NettyHttpClient.customized(Protocol.Http11, _.maxConcurrency(200)) >>>
    AwsConfig.default >>>
    DynamoDb.customized { builder =>
      builder.overrideConfiguration(
        ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(5)).build()
      )
    } >>>
    DynamoDBExecutor.live
```

The Cats Effect and `Future` modules take the same builder function: `DynamoDBExceutorF.ofCustomised` in [How to Use ZIO DynamoDB From Cats Effect](use-from-cats-effect.md), and the `buildDynamoDbClient` and `buildNettyClient` parameters of `DynamoDBExecutorF.make` in [How to Use ZIO DynamoDB From Future-Based Code](use-from-future-code.md).

## Going Further

- [How to Test Code That Uses DynamoDB](test-dynamodb-code.md) covers DynamoDB Local in tests and the in-memory test executor.
- [Architecture](../concepts/architecture.md) shows how ZIO DynamoDB sits on top of ZIO AWS and the AWS SDK.
- The AWS [DynamoDB Local guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) lists the database's options and its differences from the real service.
