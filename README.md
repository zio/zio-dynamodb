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
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery._
import zio._

object Main extends App {
  
  private final case class Person(id: Int, firstName: String)
  
  val examplePerson = Person(1, "avi")
  
  private val program = for {
    _      <- put[Person]("tableName", examplePerson).execute
    person <- get[Person]("tableName", PrimaryKey("id" -> 1)).execute
    _      <- putStrLn(s"hello ${person.firstName}")
  } yield ()
  
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    // build DynamoDB layer
    val dynamoDBLayer = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live
    program.inject(dynamoDBLayer).exitCode
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

