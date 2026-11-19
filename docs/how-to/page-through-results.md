---
id: page-through-results
title: "How to Page Through Scan and Query Results"
sidebar_label: "Page Through Results"
description: "Read a DynamoDB table or partition one page at a time with scanSome, querySome and startKey."
keywords:
  - "Pagination"
  - "LastEvaluatedKey"
  - "scanSome"
  - "querySome"
  - "startKey"
---

In this guide, we read a DynamoDB table one page at a time with `DynamoDBQuery.scanSome` and `DynamoDBQuery.querySome`. By the end, you will have a page function that a REST endpoint can call with a continuation token, and a stream that walks every page of a partition while keeping track of where it is.

We start with a single page, then fetch the next one with `DynamoDBQuery#startKey`, page through one partition with a key condition, turn the loop into a stream, and finally hand the continuation key to a client as a string.

## The Problem

DynamoDB never returns a large result in one response. A `Scan` or `Query` stops after a size or item limit and hands back a `LastEvaluatedKey`; to continue, the next request must send that key as its `ExclusiveStartKey`. A REST endpoint that serves 10 items per request has to carry that key between requests, and a background job that reads millions of items has to keep it so it can resume after a crash.

With the plain AWS SDK, every caller writes the same loop around raw `AttributeValue` maps:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, ScanRequest }
import scala.jdk.CollectionConverters._

def scanPage(
  client: DynamoDbClient,
  startKey: Option[java.util.Map[String, AttributeValue]]
): (List[(String, Int)], Option[java.util.Map[String, AttributeValue]]) = {
  val base     = ScanRequest.builder().tableName("equipment").limit(10)
  val request  = startKey.fold(base)(key => base.exclusiveStartKey(key)).build()
  val response = client.scan(request)
  val items    = response.items().asScala.toList.map { item =>
    (item.get("id").s(), item.get("year").n().toInt) // decode every attribute by hand
  }
  val next     = if (response.hasLastEvaluatedKey && !response.lastEvaluatedKey().isEmpty) Some(response.lastEvaluatedKey()) else None
  (items, next)
}
```

The decoding is manual, the "is there another page" check is easy to get wrong (the SDK returns an empty map, not a missing one), and the key type leaks raw SDK classes into every layer of the application.

ZIO DynamoDB returns decoded case classes plus a `LastEvaluatedKey`, which is an `Option[PrimaryKey]`: `Some` means "there is more", `None` means "this was the last page".

## Prerequisites

We need the core module:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The examples talk to a running DynamoDB. To start one on your machine, see [How to Connect to DynamoDB Local](connect-to-dynamodb-local.md). We assume you know how to define a model with a schema and accessors, as described in [Creating Models](../reference/high-level-api/creating-models/index.md).

The examples share these imports and a model of equipment stored in a table `equipment`, whose partition key is the equipment id and whose sort key is its year:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.{ queryAll, querySome, scanSome }
import zio.schema.{ DeriveSchema, Schema }
import zio.stream.ZStream

case class Equipment(id: String, year: Int, name: String, price: Double)
object Equipment {
  implicit val schema: Schema.CaseClass4[String, Int, String, Double, Equipment] =
    DeriveSchema.gen[Equipment]

  val (id, year, name, price) = ProjectionExpression.accessors[Equipment]
}
```

## Fetching One Page With `DynamoDBQuery.scanSome`

`DynamoDBQuery.scanSome[A](tableName, limit)` reads at most `limit` items and returns them together with the key to continue from. Its result type is `(Chunk[A], LastEvaluatedKey)`.

To read the first ten items of the table:

```scala mdoc:compile-only
val firstPage: ZIO[DynamoDBExecutor, DynamoDBError, (Chunk[Equipment], LastEvaluatedKey)] =
  scanSome[Equipment]("equipment", limit = 10).execute

val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    page          <- firstPage
    (items, next) = page
    _             <- ZIO.debug(s"read ${items.size} items, more pages: ${next.isDefined}")
  } yield ()
```

## Fetching the Next Page With `DynamoDBQuery#startKey`

`DynamoDBQuery#startKey` takes the `LastEvaluatedKey` of the previous page. Because the parameter is itself an `Option`, passing `None` starts from the beginning, so one function covers the first page and every later one.

To define a page function that a REST handler can call with the key it received from the client:

```scala mdoc:silent
def equipmentPage(startKey: LastEvaluatedKey): ZIO[DynamoDBExecutor, DynamoDBError, (Chunk[Equipment], LastEvaluatedKey)] =
  scanSome[Equipment]("equipment", limit = 10)
    .startKey(startKey)
    .execute
```

The client stops asking when the returned key is `None`.

:::warning
`limit` is the number of items DynamoDB *evaluates*, not the number it returns after a filter. With `DynamoDBQuery#filter`, a page can hold fewer than `limit` items, or none at all, while the returned key is still `Some`. Always stop on `None`, never on a short or empty page.
:::

## Paging Through One Partition With `DynamoDBQuery.querySome`

A scan reads the whole table. To page through the items of one partition, use `DynamoDBQuery.querySome` with `DynamoDBQuery#whereKey`. DynamoDB rejects a query without a key condition, so `DynamoDBQuery#whereKey` is required, and it must test the partition key for equality; the sort key condition is optional.

To page through one piece of equipment's records newer than 2020, newest first:

```scala mdoc:silent
def historyPage(id: String, startKey: LastEvaluatedKey): ZIO[DynamoDBExecutor, DynamoDBError, (Chunk[Equipment], LastEvaluatedKey)] =
  querySome[Equipment]("equipment", limit = 10)
    .whereKey(Equipment.id.partitionKey === id && Equipment.year.sortKey > 2020)
    .sortOrder(ascending = false)
    .startKey(startKey)
    .execute
```

`DynamoDBQuery#sortOrder` sets the direction of the sort key; the default is ascending.

## Walking Every Page as a Stream

When a job must process all pages but also needs the page boundaries, for example to save the last key after each page so it can resume after a restart, we can wrap the page function in `ZStream.paginateChunkZIO`. Its state is the start key; it stops when the function returns `None` as the next state.

To stream the full history of one item, page by page:

```scala mdoc:silent
def history(id: String, resumeFrom: LastEvaluatedKey): ZStream[DynamoDBExecutor, DynamoDBError, Equipment] =
  ZStream.paginateChunkZIO(resumeFrom) { startKey =>
    historyPage(id, startKey).map { case (items, lastKey) =>
      (items, lastKey.map(key => Some(key)))
    }
  }
```

`lastKey.map(key => Some(key))` turns "no more pages" (`None`) into "stop" and any real key into "continue with this key". A job that saves `lastKey` after each page can pass it back as `resumeFrom`.

## Passing the Key to a Client

A `LastEvaluatedKey` holds a `PrimaryKey`, which is an `AttrMap`. A web client needs it as a string. The optional `zio-dynamodb-json` module renders an `AttrMap` in DynamoDB JSON and parses it back:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb-json" % "@VERSION@"
```

To turn the key of the last page into a token and back:

```scala mdoc
import zio.dynamodb.json._

val lastKey: PrimaryKey = PrimaryKey("id" -> "drill-1", "year" -> 2021)

val token: String = lastKey.toJsonString
println(token)

val decoded: Either[DynamoDBError.ItemError, PrimaryKey] = parseItem(token)
```

The decoded key goes straight into `equipmentPage(Some(key))`. Treat the token as opaque: encode it (for example with Base64) before putting it in a URL, and never let a client build one by hand.

## When to Read Everything With `DynamoDBQuery.scanAll` or `DynamoDBQuery.queryAll`

If the caller wants every item and does not care about page boundaries, page handling is unnecessary. `DynamoDBQuery.scanAll` and `DynamoDBQuery.queryAll` return a `ZStream` that follows `LastEvaluatedKey` internally.

To stream all records of one item without managing keys:

```scala mdoc:compile-only
val allHistory: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    stream <- queryAll[Equipment]("equipment")
                .whereKey(Equipment.id.partitionKey === "drill-1")
                .execute
    _      <- stream.runForeach(item => ZIO.debug(item.name)).orDie
  } yield ()
```

The stream's error type is `Throwable`, not `DynamoDBError`: a request that fails while the stream is running surfaces as the AWS SDK exception. Use `DynamoDBQuery.scanSome` or `DynamoDBQuery.querySome` when you need resumable progress or a page per request; use `DynamoDBQuery.scanAll` or `DynamoDBQuery.queryAll` to consume a result set in one pass.

## Going Further

- The [scanSome](../reference/high-level-api/scan-and-query-operations/scan-some.md) and [querySome](../reference/high-level-api/scan-and-query-operations/query-some.md) reference pages list every combinator these queries accept.
- [Primary Keys](../reference/high-level-api/primary-keys.md) lists the sort key operators allowed in `DynamoDBQuery#whereKey`.
- [DynamoDBQuery](../reference/dynamodb-query.md) describes `DynamoDBQuery#filter`, `DynamoDBQuery#indexName` and `DynamoDBQuery#consistency`, which combine with paging.
