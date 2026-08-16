---
id: index
title: "Introduction to ZIO DynamoDB (series/3.x)"
sidebar_title: "ZIO DynamoDB 3.x"
---

Simple, type-safe, and efficient access to DynamoDB

@PROJECT_BADGES@

## Introduction

`series/3.x` is a major overhaul of ZIO DynamoDB 2.x: a new architecture and a new API,
built to fix specific pain points the 2.x API had accumulated. It's under active
development, not yet published to Maven Central, and the API is still evolving.

For the current, production-ready release, see the [`series/2.x`
documentation](https://zio.dev/zio-dynamodb/) instead.

## See it in action

```scala mdoc:compile-only
import cats.effect.{ IO, IOApp, Resource }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ CEInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

object Example extends IOApp.Simple {

  enum Genre derives Schema {
    case Drama, Comedy
  }

  case class Movie(id: String, genre: Genre) derives Schema

  object Movie extends CompanionOptics[Movie] {
    val id: Lens[Movie, String]   = $(_.id)
    val genre: Lens[Movie, Genre] = $(_.genre)
  }

  val client: Resource[IO, DynamoDbAsyncClient] =
    Resource.make(IO(DynamoDbAsyncClient.builder().build()))(c => IO(c.close()))

  def run: IO[Unit] =
    client.use { c =>
      given Interpreter[IO] = CEInterpreter.fromAsyncClient(c)
      for {
        _     <- put("movies", Movie("m1", Genre.Drama)).execute
        movie <- get("movies")(Movie.id.partitionKey === "m1").execute
        page  <- scan[Movie]("movies", 20).filter(Movie.genre === Genre.Drama).execute
      } yield ()
    }
}
```
`Movie.id`, `Movie.genre`, and every operator on them (`===`, `partitionKey`, `.filter`) are
plain Scala values checked against `Movie`'s schema at compile time — a typo in a field name,
or comparing `genre` against the wrong type, is a compile error, not a runtime surprise. As a
direct consequence of the zero-dependency core and the modular design, using the CE interpreter
doesn't pull ZIO — or any other effect ecosystem — onto your classpath; the same holds for the
other bundled interpreters, each pulling in only its own effect library.

The same program under ZIO — `Resource` becomes `ZLayer.scoped`, and since `.execute` takes
the interpreter as a plain implicit rather than through ZIO's environment, the layer's built
service is pulled out with `ZIO.serviceWithZIO` and bound as a `given` for the query body:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.*
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ Interpreter, ZioInterpreter }
import zio.dynamodb.ExecuteSyntax.*
import zio.dynamodb.blocks.ddbexpr.dsl.*

object ZioExample extends ZIOAppDefault {

  enum Genre derives Schema {
    case Drama, Comedy
  }

  case class Movie(id: String, genre: Genre) derives Schema

  object Movie extends CompanionOptics[Movie] {
    val id: Lens[Movie, String]   = $(_.id)
    val genre: Lens[Movie, Genre] = $(_.genre)
  }

  val interpreterLayer: ZLayer[Any, Throwable, Interpreter[Task]] =
    ZLayer.scoped {
      ZIO
        .acquireRelease(ZIO.attempt(DynamoDbAsyncClient.builder().build()))(c => ZIO.attempt(c.close()).orDie)
        .map(client => ZioInterpreter.fromAsyncClient(client): Interpreter[Task])
    }

  val program: ZIO[Interpreter[Task], Throwable, Unit] =
    ZIO.serviceWithZIO[Interpreter[Task]] { interpreter =>
      given Interpreter[Task] = interpreter
      for {
        _     <- put("movies", Movie("m1", Genre.Drama)).execute
        movie <- get("movies")(Movie.id.partitionKey === "m1").execute
        page  <- scan[Movie]("movies", 20).filter(Movie.genre === Genre.Drama).execute
      } yield ()
    }

  def run: Task[Unit] = program.provide(interpreterLayer)
}
```
Same `Movie`/`Genre` shape, same three calls, same compile-time guarantees — only the
resource-lifecycle and dependency-wiring idiom changes to match ZIO's own conventions. The
release action passed to `ZIO.acquireRelease` must be a `URIO` (cannot fail), unlike
`Resource`'s CE-effect release above — `.orDie` converts a failed close into a defect rather
than a checked error, since ZIO's scope finalizers aren't allowed to fail.

## Why the rewrite

Some specific pain points from 2.x drove the redesign:
- **2.x was ZIO-native, with other effect systems bolted on.** Cats Effect and `Future`
  support existed only as interop modules wrapping the ZIO-native core — so even if you
  never touched ZIO directly, using the CE or `Future` module still pulled the full ZIO
  runtime onto your classpath. 3.x is effect-agnostic by construction, not by wrapper: the
  same query runs unchanged under ZIO, Cats Effect, or `Future` today, with more planned,
  and non ZIO users genuinely don't pull ZIO onto their classpath (see the example above).
- **ZIO Schema 1.x was awkward to use for field access.** Declaring accessors relied on
  positional tuple-destructuring (`val (id, name) = ProjectionExpression.accessors[Person]`)
  — easy to get the order wrong, and every new field meant widening a tuple. 3.x builds on
  [ZIO Blocks Schema](https://zio.dev/zio-blocks)'s `CompanionOptics`, where each field is
  its own named, independently-declared `Lens` (see `Movie.id`/`Movie.genre` above).
- **Auto-batching and auto-parallelism broke a convention ZIO users already rely on.** 2.x's
  `Zip` combinator would silently batch or parallelize independent requests under the hood —
  but in ZIO, `zip` means sequential and `zipPar` means parallel; that distinction is
  load-bearing, and a `zip` that quietly ran in parallel contradicted it. 3.x dropped the
  magic: batching (`batchGetItem`/`batchWriteItem`) and parallelism (`zipPar`) are both
  explicit, so the query you write is the request(s) that go over the wire, and `zipPar`
  means exactly what it means everywhere else in ZIO.
- **Some operations tried to do too much.** 3.x's building blocks map one-to-one onto the AWS
  API rather than layering convenience behavior on top — less magic, more predictable control
  over exactly how your code talks to DynamoDB.

## Architecture highlights

- **Effect-independent execution model.** `DynamoDBQuery` carries no effect type of its own;
  interpreters exist today for [ZIO](https://zio.dev), [Cats
  Effect](https://typelevel.org/cats-effect/), and `scala.concurrent.Future`, with more
  planned.
- **Built-in observability, not bolted on.** A `ResponseInterceptor` fires after every
  operation with typed metadata — consumed RCU/WCU per table and index, item-collection
  size, request correlation — identical across every interpreter, so watching capacity and
  cost doesn't mean hand-parsing raw AWS responses.
- **Zero-dependency core.** `core` has no AWS SDK dependency at all — just the query
  representation and interpreter traversal logic, shared by every interpreter and every API
  layer.
- **Low-level and high-level APIs stay consistent — and interoperate.** Both compile down to
  the same `DynamoDBQuery` ADT and run through the same interpreters, so you can mix
  low-level, AWS-shaped calls with high-level, schema-derived ones in the same program.
- **The high-level API is built on [ZIO Blocks Schema](https://zio.dev/zio-blocks)**, a
  fast, type-safe foundation for schema-derived codecs and condition/key expressions.
- **Modularity is deliberate, not incidental.** Because `core` has no dependency on the
  high-level API, nothing stops a different high-level API — with different tradeoffs — from
  being built on the same zero-dependency foundation.

## What's here today

- Low-level CRUD, query, scan, batch, and transact operations, matching the AWS API shape.
- A high-level, schema-derived API (`DdbExprApi`) for CRUD, query, and scan, with type-safe
  condition and key-condition expressions.
- Interpreters for ZIO, Cats Effect, and `Future`.
- Retry policies with response-level batch retry built into query execution.

Still ahead: schema-aware batch/transact operations in the high-level API, and additional
effect-system interpreters (a Kyo interpreter is designed but not yet built).

## Try it

```scala
resolvers += "Sonatype Central Snapshots" at "https://central.sonatype.com/repository/maven-snapshots"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb-ce" % "3.0.0-SNAPSHOT" // or -zio / -future for other interpreters
)
```

It's early — the API is still moving, and we'd rather hear what's awkward now than after
1.0. Issues and feedback are welcome on [GitHub](https://github.com/zio/zio-dynamodb).
