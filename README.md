[//]: # (This file was autogenerated using `zio-sbt-website` plugin via `sbt generateReadme` command.)
[//]: # (So please do not edit it manually. Instead, change "docs/index.md" file or sbt setting keys)
[//]: # (e.g. "readmeDocumentation" and "readmeSupport".)

# ZIO DynamoDB

Simple, type-safe, and efficient access to DynamoDB

[![Development](https://img.shields.io/badge/Project%20Stage-Development-green.svg)](https://github.com/zio/zio/wiki/Project-Stages) ![CI Badge](https://github.com/zio/zio-dynamodb/workflows/CI/badge.svg) [![Sonatype Releases](https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-dynamodb_2.13.svg?label=Sonatype%20Release)](https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-dynamodb_2.13/) [![Sonatype Snapshots](https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-dynamodb_2.13.svg?label=Sonatype%20Snapshot)](https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-dynamodb_2.13/) [![javadoc](https://javadoc.io/badge2/dev.zio/zio-dynamodb-docs_2.13/javadoc.svg)](https://javadoc.io/doc/dev.zio/zio-dynamodb-docs_2.13) [![ZIO DynamoDB](https://img.shields.io/github/stars/zio/zio-dynamodb?style=social)](https://github.com/zio/zio-dynamodb)

## Introduction

ZIO DynamoDB is a library that is used for type-safe, efficient, and boilerplate free access to AWS's DynamoDB service. It provides a type-safe API for many query types and the number of type-safe APIs is expanding. ZIO DynamoDB will automatically batch queries and execute unbatchable queries in parallel.

Under the hood we use the excellent [ZIO AWS](https://zio.dev/zio-aws) library for type-safe DynamoDB access, and the awesome [ZIO Schema](https://zio.dev/zio-schema) library for schema derived codecs (see here for documentation on how to [customise these through annotations](docs/codec-customization.md)).

For an overview of the High Level API please see the [ZIO DynamoDB cheat sheet](docs/cheat-sheet.md).

## Installation

To use ZIO DynamoDB, we need to add the following lines to our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb" % "1.0.0-RC6"
)
```

### Cats Effect Interop

To use the new Cats Effect 3 interop module, we need to also add the following line to our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb-ce" "1.0.0-RC6"
)
```

For CE interop examples please see [examples sbt module](https://github.com/zio/zio-dynamodb/blob/series/2.x/examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/interop/CeInteropExample.scala).

### Read/write DynamoDB JSON
AWS tools like the CLI and Console read/write a special JSON representation of dynamoDB items. The new experimental optional `zio-dynamodb-json` module provides a way to read/write this form of JSON when working with both the High Level and Low Level API. To use this module, we need to also add the following line to our `build.sbt` file:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb-json" "1.0.0-RC6"
)
```

## Example

For examples please see [examples sbt module](https://github.com/zio/zio-dynamodb/tree/series/2.x/examples/src/main/scala/zio/dynamodb/examples). Below is `Main.scala` from that module:

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

For examples on how to use the DynamoDBLocal in memory database please see the [integration tests](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiCrudSpec.scala)
and [DynamoDBLocalMain](https://github.com/zio/zio-dynamodb/blob/series/2.x/examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/DynamoDBLocalMain.scala) .
Note before you run these you must first run the DynamoDBLocal docker container using the provided docker-compose file:

```
docker compose -f docker/docker-compose.yml up -d
```

Don't forget to shut down the container after you have finished

```
docker compose -f docker/docker-compose.yml down
```

## Resources
- [Introducing ZIO DynamoDB by Avinder Bahra & Adam Johnson](https://www.youtube.com/watch?v=f68-69eA8Vc&t=33s) - DynamoDB powers many cloud-scale applications, with its robust horizontal scalability and uptime. Yet, interacting with the Java SDK is error-prone and tedious. In this presentation, Avinder Bahra presents ZIO DynamoDB, a new library by Avi and Adam Johnson designed to make interacting with DynamoDB easy, type-safe, testable, and productive.
- [Introducing The ZIO DynamoDB Type-Safe API by Avinder Bahra](https://www.youtube.com/watch?v=Qte4WUfHQ3g&t=10s) - Last year, Adam Johnson and Avinder released ZIO DynamoDB, a new Scala library that significantly reduces boilerplate when compared to working directly with AWS client libraries. However, there was still work to be done to improve type safety. In this talk, Avinder introduces a new type-safe API that can prevent many errors at compile time while remaining user-friendly.

## Documentation

Learn more on the [ZIO DynamoDB homepage](https://zio.dev/zio-dynamodb/)!

## Contributing

For the general guidelines, see ZIO [contributor's guide](https://zio.dev/contributor-guidelines).

## Code of Conduct

See the [Code of Conduct](https://zio.dev/code-of-conduct)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].

[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"

## License

[License](LICENSE)
