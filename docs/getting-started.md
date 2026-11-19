---
id: getting-started
title: "Getting Started"
---

This page takes us from an empty sbt project to a program that writes an item to DynamoDB and reads it back. We run it against [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html), a local copy of DynamoDB, so no AWS account is needed.

## Prerequisites

We need Java 17 or newer, sbt, and Docker (to run DynamoDB Local).

## Add the Dependency

ZIO DynamoDB is published for Scala 2.13 and Scala 3. Add it to `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The optional modules are listed on the [Introduction](index.md#modules) page.

## Start DynamoDB Local

The ZIO DynamoDB repository ships a [`docker-compose.yml`](https://github.com/zio/zio-dynamodb/blob/series/2.x/docker/docker-compose.yml) that runs DynamoDB Local in memory on port 8000. Save it as `docker/docker-compose.yml` in our project, then start it:

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Write the Program

The program below defines a `Person` model, creates a `person` table, writes one person, reads it back and deletes the table again:

```scala mdoc:compile-only
import java.net.URI

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb._
import zio.dynamodb.syntax._
import zio.schema.{ DeriveSchema, Schema }

object Main extends ZIOAppDefault {

  case class Person(id: Int, firstName: String)
  object Person {
    implicit val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]

    val (id, firstName) = ProjectionExpression.accessors[Person]
  }

  private val program =
    for {
      _      <- DynamoDBQuery
                  .createTable("person", KeySchema("id"), BillingMode.PayPerRequest)(
                    AttributeDefinition.attrDefnNumber("id")
                  )
                  .execute
      _      <- DynamoDBQuery.put("person", Person(1, "Avi")).execute
      person <- DynamoDBQuery.get("person")(Person.id.partitionKey === 1).execute.maybeFound
      _      <- Console.printLine(s"found: $person")
      _      <- DynamoDBQuery.deleteTable("person").execute
    } yield ()

  // A DynamoDB client that talks to DynamoDB Local instead of AWS
  private val dynamoDbLocal: ZLayer[Any, Throwable, DynamoDb] =
    NettyHttpClient.default >>> AwsConfig.default >>> DynamoDb.customized { builder =>
      builder
        .endpointOverride(URI.create("http://localhost:8000"))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
    }

  override def run =
    program.provide(dynamoDbLocal, DynamoDBExecutor.live)
}
```

Running it with `sbt run` prints `found: Some(Person(1,Avi))`.

The program shows the three ideas we use everywhere in ZIO DynamoDB:

- `Person.schema` lets the library turn a `Person` into a DynamoDB item and back, and `ProjectionExpression.accessors` gives us `Person.id` and `Person.firstName`, typed handles on the model's fields.
- `DynamoDBQuery.put`, `DynamoDBQuery.get` and `DynamoDBQuery.createTable` only describe requests. `DynamoDBQuery#execute` turns each one into a ZIO effect that needs a `DynamoDBExecutor`.
- `DynamoDBExecutor.live` builds the executor from a ZIO AWS `DynamoDb` client. Pointing that client at real AWS instead of DynamoDB Local only changes the `dynamoDbLocal` layer; see [How to Connect to DynamoDB Local or Customise the AWS Client](how-to/connect-to-dynamodb-local.md).

`DynamoDBQuery.get` returns an `Either` that is `Left` when the item is missing or cannot be decoded; `maybeFound` (from `zio.dynamodb.syntax._`) turns a missing item into `None`.

When we are done, we stop DynamoDB Local:

```bash
docker compose -f docker/docker-compose.yml down
```

## Next Steps

- [Tutorial: Build an Order Store](tutorials/build-an-order-store.md) builds a larger application step by step: models, tables, queries and a transaction.
- [High Level API](concepts/high-level-api.md) explains how schemas and projection expressions fit together.
- The [Cheat Sheet](reference/cheat-sheet.md) maps each DynamoDB operation to its ZIO DynamoDB call.
