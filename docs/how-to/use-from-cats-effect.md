---
id: use-from-cats-effect
title: "How to Use ZIO DynamoDB From Cats Effect"
sidebar_label: "Use From Cats Effect"
description: "Run ZIO DynamoDB queries as Cats Effect F[_] effects and FS2 streams with the zio-dynamodb-ce interop module."
keywords:
  - "Cats Effect"
  - "Interop"
  - "FS2"
  - "DynamoDBExceutorF"
  - "executeToF"
---

In this guide, we use ZIO DynamoDB from a Cats Effect 3 application. By the end, you will have a program, polymorphic in `F[_]`, that creates a table, writes and reads items, scans them as an FS2 stream, and bulk-loads items from a stream, with no ZIO types in its signatures.

## The Problem

ZIO DynamoDB's queries run with `DynamoDBQuery#execute`, which returns a ZIO effect and, for scans and queries, a ZIO stream. A Cats Effect application wants `F[A]` and `fs2.Stream[F, A]` instead, and it manages resources with `Resource`. Converting by hand means running each ZIO effect on a ZIO runtime, building and closing the AWS client ourselves, and translating every stream.

The `zio-dynamodb-ce` module does this conversion: it builds the executor as a `Resource[F, _]` and adds an `executeToF` method to every query.

## Prerequisites

Add the interop module next to the core module:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb"    % "@VERSION@",
  "dev.zio" %% "zio-dynamodb-ce" % "@VERSION@"
)
```

The examples share these imports and a `Person` model stored in a `Person` table keyed by `id`:

```scala mdoc:silent
import java.net.URI

import cats.effect.{ Async, IO, IOApp, Resource }
import cats.effect.std.{ Console, Dispatcher }
import cats.syntax.all._
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.interop.ce.syntax._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]

  val (id, name) = ProjectionExpression.accessors[Person]
}
```

## Creating the Executor as a Resource

The module's executor class is `DynamoDBExceutorF[F]` (the class name is spelt that way in the library). Its companion object has two constructors, both returning a `Resource[F, DynamoDBExceutorF[F]]` that builds the AWS client on acquire and closes it on release. `DynamoDBExceutorF.of[F]` uses the AWS SDK defaults, which suits real AWS, and `DynamoDBExceutorF.ofCustomised[F](f)` lets `f` adjust the AWS SDK's `DynamoDbAsyncClientBuilder`, for example to point it at DynamoDB Local.

Only AWS SDK types appear in the customisation function, not ZIO AWS ones. To build an executor for DynamoDB Local:

```scala mdoc:silent
def localExecutor[F[_]: Async]: Resource[F, DynamoDBExceutorF[F]] =
  DynamoDBExceutorF.ofCustomised[F] { builder =>
    builder
      .endpointOverride(URI.create("http://localhost:8000"))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
  }
```

## Running Queries as F Effects

With a `DynamoDBExceutorF[F]` in implicit scope, `executeToF` runs any query. A query that returns a value in ZIO now returns `F` of that value, and a query that returns a ZIO stream (`scanAll`, `queryAll`) now returns `F` of an `fs2.Stream[F, A]`. A failed query fails `F` with the `DynamoDBError`, and cancelling `F` interrupts the running query.

To create a table, write and read a person, and stream the scanned items:

```scala mdoc:silent
def crud[F[_]](implicit F: Async[F], executor: DynamoDBExceutorF[F]): F[Unit] = {
  val console = Console.make[F]

  for {
    _      <- createTable("Person", KeySchema("id"), BillingMode.PayPerRequest)(
                AttributeDefinition.attrDefnString("id")
              ).executeToF
    _      <- put("Person", Person("avi", "Avinder")).executeToF
    result <- get("Person")(Person.id.partitionKey === "avi").executeToF
    _      <- console.println(s"found=$result")
    people <- scanAll[Person]("Person")
                .filter(Person.name.beginsWith("Avi"))
                .executeToF
    _      <- people.evalTap(person => console.println(s"scanned $person")).compile.drain
  } yield ()
}
```

## Loading and Reading Items From an FS2 Stream

The [streaming batch helpers](../reference/streaming-batch-helpers.md) have Cats Effect versions that take and return FS2 streams: `batchWriteFromStreamF`, `batchReadFromStreamF` and `batchReadItemFromStreamF`. Besides the executor, they need an implicit `cats.effect.std.Dispatcher[F]`, which we obtain as a resource.

To write a stream of people in batches of 25, then read them back by key in batches of 100:

```scala mdoc:silent
def bulk[F[_]](
  implicit F: Async[F],
  executor: DynamoDBExceutorF[F],
  dispatcher: Dispatcher[F]
): F[List[Either[DynamoDBError.ItemError.DecodingError, (Person, Option[Person])]]] = {
  val input = fs2.Stream(Person("1", "Ann"), Person("2", "Bob")).covary[F]

  for {
    _     <- batchWriteFromStreamF(input)(person => put("Person", person)).compile.drain
    found <- batchReadFromStreamF("Person", input)(person => Person.id.partitionKey === person.id).compile.toList
  } yield found
}
```

`batchReadFromStreamF` pairs each input element with the item it found, or `None`; `batchReadItemFromStreamF` does the same with Low Level API items and a `PrimaryKey`.

## Putting the Program Together

The application acquires the executor and a dispatcher, makes them implicit, and runs both parts:

```scala mdoc:compile-only
object CeInteropApp extends IOApp.Simple {

  def program[F[_]: Async]: F[Unit] =
    (localExecutor[F], Dispatcher.parallel[F]).tupled.use { case (exec, disp) =>
      implicit val executor: DynamoDBExceutorF[F] = exec
      implicit val dispatcher: Dispatcher[F]      = disp

      for {
        _ <- crud[F]
        _ <- bulk[F]
        _ <- deleteTable("Person").executeToF
      } yield ()
    }

  val run: IO[Unit] = program[IO]
}
```

## Going Further

- The repository's [CeInteropExample](https://github.com/zio/zio-dynamodb/blob/series/2.x/examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/interop/CeInteropExample.scala) and [CeInteropStreamUtilsExample](https://github.com/zio/zio-dynamodb/blob/series/2.x/examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/interop/CeInteropStreamUtilsExample.scala) run against DynamoDB Local.
- [How to Connect to DynamoDB Local or Customise the AWS Client](connect-to-dynamodb-local.md) explains the client settings used above.
- [How to Use ZIO DynamoDB From Future-Based Code](use-from-future-code.md) covers applications without an effect library.
