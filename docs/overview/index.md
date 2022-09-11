---
id: overview_index
title: "Overview"
---

ZIO-DynamoDB is a library that is used for type-safe, efficient, and boilerplate free access to AWS's DynamoDB service. It provides a type-safe API for many query types and the number of type-safe APIs is expanding.

# Getting Started

## Add the dependency to your build.sbt file

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % <version>
```

### Read & write data to/from DynamoDB

```scala
import zio._
import zio.clock.Clock
import zio.schema.{ DeriveSchema, Schema }
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import io.github.vigoo.zioaws.dynamodb
import io.github.vigoo.zioaws.dynamodb.DynamoDb
import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.http4s

final case class Student(email: String, subject: String)
object Student {
  implicit lazy val schema: Schema[Student] = DeriveSchema.gen[Student]
}

object Main extends App {
  private val liveDynamoDbLayer: ZLayer[Any, Throwable, DynamoDb] =
    http4s.default >>> config.default >>> dynamodb.live

  // Assuming table "student" exists with email as HASH key and subject as the RANGE key
  val avi = Student("avi@gmail.com", "maths")
  val adam = Student("adam@gmail.com", "english")

  def run(args: List[String]) = {
    (for {
      _ <- (put("student", avi) zip put("student", adam)).execute
      listOfStudentsOrError <- forEach(List(avi, adam)) { student =>
        get[Student]("student",
          PrimaryKey("email" -> student.email, "subject" -> student.subject)
        )}.execute
    } yield ())
      .provideLayer((liveDynamoDbLayer ++ Clock.live) >>> DynamoDBExecutor.live)
      .exitCode
  }
}
```
