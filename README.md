# zio-dynamodb

| Project Stage | CI | Release | Snapshot | Discord |
| --- | --- | --- | --- | --- |
| [![Project stage][Stage]][Stage-Page] | ![CI][Badge-CI] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Badge-Discord]][Link-Discord] |

# Summary
Simple, type-safe, and efficient access to DynamoDB

# Documentation

### Getting Started

```sbt
// only snapshots are published at the moment
resolvers += Resolver.sonatypeRepo("snapshots")

// add zio-dynamodb to your dependencies - lookup the latest snapshot version here https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-dynamodb_2.13/
libraryDependencies ++= Seq(
  // ...
  "dev.zio"               %% "zio-dynamodb"          % "0.0.1<LATEST_VERSION>"
)
```

For examples please see [examples sbt module](examples/src/main/scala/zio/dynamodb/examples). Below is `Main.scala` from that module.

```scala
package zio.dynamodb.examples

import io.github.vigoo.zioaws.http4s
import zio.{ App, ExitCode, Has, URIO, ZLayer }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb
import zio.clock.Clock

object Main extends ZIOAppDefault {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema[Person] = DeriveSchema.gen[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- zio.Console.printLine(s"hello $person")
  } yield ()

  override def run = {

    val dynamoDbLayer =
      http4s.Http4sClient.default >>> config.AwsConfig.default >>> dynamodb.DynamoDb.live // uses real AWS dynamodb
    val executorLayer = dynamoDbLayer >>> DynamoDBExecutor.live

    program.provide(executorLayer)
  }
}

```

For examples on how to use the DynamoDBLocal in memory database please see the [integration tests](dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala)
and [StudentZioDynamoDbExample](examples/src/main/scala/zio/dynamodb/examples/dynamodblocal/StudentZioDynamoDbExample.scala)

Under the hood we use the excellent [ZIO AWS](https://github.com/zio/zio-aws) library for type-safe DynamoDB access, and
the awesome [ZIO Schema](https://github.com/zio/zio-schema) library for schema derived codecs (see here for documentation
on how to [customise these through annotations](docs/usecases/codec-customisation.md)).

Microsite content to come soon.


[ZIO DynamoDB Microsite](https://zio.github.io/zio-dynamodb/)

# Contributing
[Documentation for contributors](https://zio.github.io/zio-dynamodb/docs/about/about_contributing)

## Code of Conduct

See the [Code of Conduct](https://zio.github.io/zio-dynamodb/docs/about/about_coc)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].


# License
[License](LICENSE)

[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-dynamodb_2.12.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-dynamodb_2.12.svg "Sonatype Snapshots"
[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-dynamodb_2.12/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-dynamodb_2.12/ "Sonatype Snapshots"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"
[Badge-CI]: https://github.com/zio/zio-dynamodb/workflows/CI/badge.svg
[Stage]: https://img.shields.io/badge/Project%20Stage-Experimental-yellow.svg
[Stage-Page]: https://github.com/zio/zio/wiki/Project-Stages

