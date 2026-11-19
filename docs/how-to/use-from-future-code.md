---
id: use-from-future-code
title: "How to Use ZIO DynamoDB From Future-Based Code"
sidebar_label: "Use From Future-Based Code"
description: "Run ZIO DynamoDB queries as Scala Futures with the zio-dynamodb-future interop module, for applications that use no effect library."
keywords:
  - "Future"
  - "Interop"
  - "Play Framework"
  - "DynamoDBExecutorF"
  - "executeToF"
---

In this guide, we use ZIO DynamoDB from an application built on Scala `Future`s, such as a Play application. By the end, you will have an executor created once at start-up, queries that return `Future`s, and a clean shutdown.

## The Problem

`DynamoDBQuery#execute` returns a ZIO effect that needs a `DynamoDBExecutor` and an AWS client, both built from ZIO layers. A `Future`-based application has no ZIO runtime to build those layers or run the effect, and it would have to manage the AWS client's lifecycle itself.

The `zio-dynamodb-future` module wraps all of this in one object, `DynamoDBExecutorF`, which owns a ZIO runtime with the executor inside it, and adds a `DynamoDBQuery#executeToF` method that runs a query on that runtime and returns a `Future`.

## Prerequisites

Add the interop module next to the core module:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb"        % "@VERSION@",
  "dev.zio" %% "zio-dynamodb-future" % "@VERSION@"
)
```

The examples use a `Person` model stored in a `Person` table keyed by `id`:

```scala mdoc:silent
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.interop.future.DynamoDBExecutorF
import zio.dynamodb.interop.future.syntax._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]

  val (id, name) = ProjectionExpression.accessors[Person]
}
```

## Creating the Executor

`DynamoDBExecutorF.make` builds the AWS client and the executor, and starts the runtime that holds them. All its parameters have defaults:

```scala
object DynamoDBExecutorF {
  def make(
    protocol: Protocol = Protocol.Http11,
    buildNettyClient: NettyNioAsyncHttpClient.Builder => NettyNioAsyncHttpClient.Builder = identity,
    buildDynamoDbClient: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
  ): DynamoDBExecutorF
}
```

`protocol` selects the HTTP protocol of the Netty client, `buildNettyClient` adjusts the AWS SDK's Netty HTTP client, and `buildDynamoDbClient` adjusts the DynamoDB client itself. With the defaults, the client uses the AWS SDK's default credentials and region, which suits real AWS. To point it at DynamoDB Local instead:

```scala mdoc:silent
import java.net.URI

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region

def localExecutor(): DynamoDBExecutorF =
  DynamoDBExecutorF.make(
    buildDynamoDbClient = _.endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
  )
```

`DynamoDBExecutorF.make` starts the runtime immediately, so we create the executor once, at application start-up, and share it.

## Running Queries as Futures

With a `DynamoDBExecutorF` in implicit scope, `DynamoDBQuery#executeToF` runs a query and returns a `zio.CancelableFuture`, a `Future` that can also cancel the running query. A failed query fails the `Future` with the `DynamoDBError`.

To create a table, write and read a person, and drop the table:

```scala mdoc:silent
import scala.concurrent.{ ExecutionContext, Future }

def program(implicit executor: DynamoDBExecutorF, ec: ExecutionContext): Future[Unit] =
  for {
    _      <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                AttributeDefinition.attrDefnString("id")
              ).executeToF
    _      <- put("Person", Person("avi", "Avinder")).executeToF
    result <- get("Person")(Person.id.partitionKey === "avi").executeToF
    _       = println(s"found=$result")
    _      <- deleteTable("Person").executeToF
  } yield ()
```

Queries that return a ZIO stream, such as `DynamoDBQuery.scanAll`, still return the stream inside the `Future`; use `DynamoDBQuery.scanSome` and `DynamoDBQuery.querySome` to read pages instead (see [How to Page Through Scan and Query Results](page-through-results.md)).

## Shutting Down

`DynamoDBExecutorF#close` shuts the runtime down and releases the AWS client. We call it once, when the application stops, after the last query has finished.

A complete application creates the executor, runs the program and closes the executor when the program completes:

```scala mdoc:compile-only
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object FutureInteropApp extends App {
  implicit val executor: DynamoDBExecutorF = localExecutor()

  val programWithClose = program.andThen { case _ => executor.close() }

  Await.result(programWithClose, 30.seconds)
}
```

## Going Further

- The repository's [FutureInteropExample](https://github.com/zio/zio-dynamodb/blob/series/2.x/examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/interop/FutureInteropExample.scala) runs against DynamoDB Local.
- [How to Connect to DynamoDB Local or Customise the AWS Client](connect-to-dynamodb-local.md) explains the client settings used above.
- [How to Use ZIO DynamoDB From Cats Effect](use-from-cats-effect.md) covers Cats Effect applications.
