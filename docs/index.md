---
id: index
title: "Introduction to ZIO DynamoDB"
sidebar_title: "ZIO DynamoDB"
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
import zio.aws.core.config
import zio.aws.{ dynamodb, netty }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor }
import zio.schema.{ DeriveSchema, Schema }
import zio.ZIOAppDefault
import zio.dynamodb.ProjectionExpression

object Main extends ZIOAppDefault {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]

    val (id, firstName) = ProjectionExpression.accessors[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("personTable", examplePerson).execute
    person <- get("personTable")(Person.id.partitionKey === 1).execute
    _      <- zio.Console.printLine(s"hello $person")
  } yield ()

  override def run =
    program.provide(
      netty.NettyHttpClient.default,
      config.AwsConfig.default, // uses real AWS dynamodb
      dynamodb.DynamoDb.live,
      DynamoDBExecutor.live
    )
}
```

For examples on how to use the DynamoDBLocal in memory database please see the [integration tests](../dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala)
and [StudentZioDynamoDbExample](../examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/StudentZioDynamoDbExample.scala)

## Resources
- [Introducing ZIO DynamoDB by Avinder Bahra & Adam Johnson](https://www.youtube.com/watch?v=f68-69eA8Vc&t=33s) - DynamoDB powers many cloud-scale applications, with its robust horizontal scalability and uptime. Yet, interacting with the Java SDK is error-prone and tedious. In this presentation, Avinder Bahra presents ZIO DynamoDB, a new library by Avi and Adam Johnson designed to make interacting with DynamoDB easy, type-safe, testable, and productive.
- [Introducing The ZIO DynamoDB Type-Safe API by Avinder Bahra](https://www.youtube.com/watch?v=Qte4WUfHQ3g&t=10s) - Last year, Adam Johnson and Avinder released ZIO DynamoDB, a new Scala library that significantly reduces boilerplate when compared to working directly with AWS client libraries. However, there was still work to be done to improve type safety. In this talk, Avinder introduces a new type-safe API that can prevent many errors at compile time while remaining user-friendly.
