---
id: index
title: "Overview"
---

Simple, type-safe, and efficient access to DynamoDB

@PROJECT_BADGES@

## Introduction

ZIO DynamoDB is a library that is used for type-safe, efficient, and boilerplate free access to AWS's DynamoDB service. It provides a type-safe API for many query types and the number of type-safe APIs is expanding. ZIO DynamoDB will automatically batch queries and execute unbatchable queries in parallel.

Under the hood we use the excellent [ZIO AWS](https://zio.dev/zio-aws) library for type-safe DynamoDB access, and the awesome [ZIO Schema](https://zio.dev/zio-schema) library for schema derived codecs (see here for documentation on how to [customise these through annotations](codec-customization.md)).

## Installation

To use ZIO DynamoDB, we need to add the following lines to our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb" % "@VERSION@"
)
```

## Example

For examples please see [examples sbt module](../examples/src/main/scala/zio/dynamodb/examples). Below is `Main.scala` from that module:

```scala
import io.github.vigoo.zioaws.http4s
import zio.{ App, ExitCode, Has, URIO, ZLayer }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb
import zio.clock.Clock

object Main extends App {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema[Person] = DeriveSchema.gen[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- zio.console.putStrLn(s"hello $person")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val dynamoDbLayer = http4s.default >>> config.default >>> dynamodb.live // uses real AWS dynamodb
    val executorLayer = (dynamoDbLayer ++ ZLayer.identity[Has[Clock.Service]]) >>> DynamoDBExecutor.live

    program.provideCustomLayer(executorLayer).exitCode
  }
}
```

For examples on how to use the DynamoDBLocal in memory database please see the [integration tests](../dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala)
and [StudentZioDynamoDbExample](../examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/StudentZioDynamoDbExample.scala)
