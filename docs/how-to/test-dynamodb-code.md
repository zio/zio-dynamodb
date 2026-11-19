---
id: test-dynamodb-code
title: "How to Test Code That Uses DynamoDB"
sidebar_label: "Test DynamoDB Code"
description: "Test ZIO DynamoDB code against DynamoDB Local, with the in-memory test executor, and check data mappings with the DynamoDB JSON module."
keywords:
  - "Testing"
  - "DynamoDB Local"
  - "Integration Tests"
  - "ZIO Test"
  - "DynamoDBExecutor"
---

In this guide, we test a small repository of people at three levels: full-stack integration tests against a local DynamoDB, fast unit tests against an in-memory executor, and unit tests of how a model is stored. By the end, you will know which kind of test fits which question and how to wire each one.

## The Problem

Code that uses ZIO DynamoDB depends on a `DynamoDBExecutor`, and the default one sends every query to AWS. Tests that hit a real AWS account are slow, cost money, need credentials, and can collide with each other or with real data. On the other hand, a mock that only records calls cannot tell us whether a condition expression, a key condition or a codec actually does what we meant, because DynamoDB evaluates those.

Because every query only needs a `DynamoDBExecutor` from the environment, we can choose the executor per test without touching the code under test.

## Prerequisites

The examples use the core module and, for mapping tests, the optional JSON module:

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb"      % "@VERSION@",
  "dev.zio" %% "zio-dynamodb-json" % "@VERSION@" % Test,
  "dev.zio" %% "zio-test"          % "@ZIO_VERSION@" % Test,
  "dev.zio" %% "zio-test-sbt"      % "@ZIO_VERSION@" % Test
)
```

The code under test is a repository of people stored in a `people` table keyed by `id`:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, nicknames: Set[String])
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Set[String], Person] = DeriveSchema.gen[Person]

  val (id, name, nicknames) = ProjectionExpression.accessors[Person]
}

object PeopleRepository {
  def save(person: Person): ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
    DynamoDBQuery.put("people", person).execute.unit

  def find(id: String): ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] = {
    import zio.dynamodb.syntax._
    DynamoDBQuery.get("people")(Person.id.partitionKey === id).execute.maybeFound
  }
}
```

## Integration Testing Against DynamoDB Local

The most faithful tests run the real queries against a local DynamoDB. Two local options exist; use whichever suits your environment. [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html) is a stand-alone database from AWS, also available as the `amazon/dynamodb-local` Docker image. The ZIO DynamoDB build runs its own integration tests against this image. [LocalStack](https://docs.localstack.cloud/) is a Docker image that emulates many AWS services, DynamoDB among them.

[How to Connect to DynamoDB Local or Customise the AWS Client](connect-to-dynamodb-local.md) shows how to start DynamoDB Local and build a `DynamoDBExecutor` layer that points at it. For tests, we add a scoped layer that creates the table before the tests and drops it afterwards, so every run starts clean:

```scala mdoc:silent
import zio.dynamodb.DynamoDBQuery.{ createTable, deleteTable }

val peopleTable: ZLayer[DynamoDBExecutor, Throwable, Unit] =
  ZLayer.scoped(
    ZIO.acquireRelease(
      createTable("people", KeySchema("id"), BillingMode.PayPerRequest)(
        AttributeDefinition.attrDefnString("id")
      ).execute
    )(_ => deleteTable("people").execute.orDie)
  )
```

A test body is then an ordinary effect over the repository. To check that a saved person loads back unchanged:

```scala mdoc:silent
val savesAndLoads: ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  for {
    _     <- PeopleRepository.save(Person("1", "Avi", Set("A")))
    found <- PeopleRepository.find("1")
  } yield found.contains(Person("1", "Avi", Set("A")))
```

In a ZIO Test spec, the test asserts on that result, and the suite gets its dependencies with `Spec#provideShared`, which creates the table once for the whole suite; `Spec#provide` would create and drop it around every test. Here `localExecutor` stands for the DynamoDB Local layer from the guide linked above:

```scala mdoc:compile-only
import zio.test._

def integrationSpec(localExecutor: ZLayer[Any, Throwable, DynamoDBExecutor]): Spec[Any, Any] =
  suite("PeopleRepository against DynamoDB Local")(
    test("saves and loads a person") {
      savesAndLoads.map(ok => assertTrue(ok))
    }
  ).provideShared(localExecutor, peopleTable)
```

## Unit Testing With the In-Memory Executor

For logic that only stores and loads whole items, `DynamoDBExecutor.test` replaces DynamoDB with an in-memory map, so tests need no Docker and run in milliseconds. It ignores expressions such as conditions and key conditions, and does not support updates or transactions, so code that relies on them belongs in the integration tests above.

To run the same check against the in-memory executor, we provide it with the table name and partition key:

```scala mdoc
Unsafe.unsafe { implicit unsafe =>
  Runtime.default.unsafe.run(savesAndLoads.provide(DynamoDBExecutor.test("people" -> "id"))).getOrThrowFiberFailure()
}
```

In a spec, `Spec#provide` with `DynamoDBExecutor.test("people" -> "id")` gives every test a fresh, empty database:

```scala mdoc:compile-only
import zio.test._

object PeopleRepositorySpec extends ZIOSpecDefault {
  def spec =
    suite("PeopleRepository in memory")(
      test("saves and loads a person") {
        savesAndLoads.map(ok => assertTrue(ok))
      },
      test("finds nothing in an empty table") {
        PeopleRepository.find("missing").map(found => assertTrue(found.isEmpty))
      }
    ).provide(DynamoDBExecutor.test("people" -> "id"))
}
```

[Test Executor](../reference/test-executor.md) documents how to seed tables, inspect stored items and recorded queries, and exactly which operations the fake supports.

## Unit Testing Data Mappings

Some bugs live in the mapping itself: a renamed field, a changed sum type annotation, or a set that DynamoDB stores differently from a list. The optional `zio-dynamodb-json` module renders a value exactly as ZIO DynamoDB stores it, using the same codecs, so a plain unit test can pin the stored form:

```scala mdoc:silent
import zio.dynamodb.json._

val stored = Person("1", "Avi", Set("A")).toJsonString
```

The stored form, and the value `parse` reads back from it:

```scala mdoc
println(stored)
parse[Person](stored)
```

Comparing `stored` with an expected JSON string catches a change in the stored form before it reaches a real table, and `parse` checks that the stored form reads back. [ZIO DynamoDB JSON Module](../reference/zio-dynamodb-json.md) describes both methods.

## Going Further

- [Test Executor](../reference/test-executor.md) is the reference for the in-memory executor.
- [How to Customize How Sum Types Are Stored](customize-sum-type-encoding.md) shows mapping changes that are worth pinning with JSON tests.
- The ZIO DynamoDB [integration tests](https://github.com/zio/zio-dynamodb/tree/series/2.x/dynamodb/src/it/scala/zio/dynamodb) are a large set of examples of tests against DynamoDB Local.
