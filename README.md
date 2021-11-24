# zio-dynamodb

| Project Stage | CI | Release | Snapshot | Discord |
| --- | --- | --- | --- | --- |
| [![Project stage][Stage]][Stage-Page] | ![CI][Badge-CI] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Badge-Discord]][Link-Discord] |

# Summary
Simple, type-safe, and efficient access to DynamoDB

# Documentation

### Getting Started

```sbt
// add zio-dynamodb to your dependencies

```

```scala
import io.github.vigoo.zioaws.http4s
import zio.{ App, ExitCode, URIO }
import zio.dynamodb.DynamoDBQuery.{ get, put }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.dynamodb

object Main extends App {

  final case class Person(id: Int, firstName: String)
  object Person {
    implicit lazy val schema: Schema[Person] = DeriveSchema.gen[Person]
  }
  val examplePerson = Person(1, "avi")

  private val program = for {
    _      <- put[Person]("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- zio.console.putStrLn(s"hello $person")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    // build DynamoDB layer
    val dynamoDBLayer = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

    program.provideCustomLayer(dynamoDBLayer).exitCode
  }
}
```

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

