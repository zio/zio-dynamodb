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

```scala
import cats.effect.{ IO, IOApp }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.{ CEInterpreter, Interpreter }
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.dsl._

sealed trait Genre
object Genre {
  case object Drama  extends Genre
  case object Comedy extends Genre
  implicit val schema: Schema[Genre] = Schema.derived
}

final case class Movie(id: String, genre: Genre)
object Movie extends CompanionOptics[Movie] {
  implicit val schema: Schema[Movie] = Schema.derived
  val id: Lens[Movie, String]        = $(_.id)
  val genre: Lens[Movie, Genre]      = $(_.genre)
}

object Example extends IOApp.Simple {
  implicit val interpreter: Interpreter[IO] = CEInterpreter.fromAsyncClient(DynamoDbAsyncClient.builder().build())

  def run: IO[Unit] =
    for {
      _     <- put("movies", Movie("m1", Genre.Drama)).execute
      movie <- get("movies")(Movie.id.partitionKey === "m1").execute
      page  <- scan[Movie]("movies", 20).filter(Movie.genre === Genre.Drama).execute
    } yield ()
}
```
`Movie.id`, `Movie.genre`, and every operator on them (`===`, `partitionKey`, `.filter`) are
plain Scala values checked against `Movie`'s schema at compile time — a typo in a field name,
or comparing `genre` against the wrong type, is a compile error, not a runtime surprise. And
that's genuinely `cats.effect.IO` throughout, not a `Future`/ZIO shim wearing a CE-shaped hat —
`CEInterpreter` implements the library's effect primitives directly against `IO`. The `dev.zio`
naming reflects where the project is hosted, not a hidden ZIO runtime dependency: `core` has no
effect-system dependency at all, and the schema layer (`zio.blocks.schema`) is metaprogramming,
not an effect system — same category as Circe or shapeless, unrelated to which effect type you
run queries in.

## Why the rewrite

Three specific pain points from 2.x drove the redesign:

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
