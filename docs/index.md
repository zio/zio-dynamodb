---
id: index
title: "Introduction to ZIO DynamoDB"
sidebar_label: "Introduction"
---

Simple, type-safe, and efficient access to DynamoDB

@PROJECT_BADGES@

> **ZIO DynamoDB 3 is in development** on the [`series/3.x`](https://github.com/zio/zio-dynamodb/tree/series/3.x) branch. Highlights of the rewrite:
>
> - **Effect-neutral**: no longer tied to ZIO - the same queries run on ZIO, Cats Effect or `Future`, each through its own interpreter.
> - **Modular**: a small core with no AWS SDK dependency, plus a module per effect and for schema support, so you depend only on what you use.
> - **High-Level API based on [ZIO Blocks Schema](https://github.com/zio/zio-blocks)**, with derived codecs and type-safe expressions built from optics.
>
> Snapshots are published from that branch. This branch (2.x) is in maintenance mode.

## Introduction

ZIO DynamoDB is a library for type-safe, efficient and boilerplate-free access to AWS DynamoDB. We describe a read or write as a `DynamoDBQuery` value, combine queries, and run them with `DynamoDBQuery#execute`, which returns a ZIO effect. When several queries are combined, ZIO DynamoDB batches the ones DynamoDB can batch and runs the rest in parallel.

The library offers two APIs. The **High Level API** works with our own case classes and sealed traits. It derives the DynamoDB codecs from a [ZIO Schema](https://zio.dev/zio-schema) and builds type-checked expressions from the model's fields, so a typo in a field name or a comparison with the wrong type does not compile. The **Low Level API** works with DynamoDB items (`Item`, an attribute map) and string paths such as `$("address.city")`, mirroring the AWS API one to one.

Under the hood, ZIO DynamoDB calls DynamoDB through [ZIO AWS](https://zio.dev/zio-aws), which wraps the AWS SDK for Java v2.

## Installation

ZIO DynamoDB is published for Scala 2.13 and Scala 3 and runs on the JVM, Java 17 or newer. To use it, add the core module to `build.sbt`:

```scala
libraryDependencies ++= Seq(
  // Core: High Level and Low Level APIs, codecs and the DynamoDB executor (JVM)
  "dev.zio" %% "zio-dynamodb" % "@VERSION@"
)
```

The core module brings in ZIO, ZIO Streams, ZIO Schema and the ZIO AWS DynamoDB client, so no other dependency is needed to talk to DynamoDB. Continue with [Getting Started](getting-started.md) to write and run a first program.

## Modules

The other modules are optional. Each one depends on the core module.

### Effect Interop

These modules run ZIO DynamoDB queries from code that does not use ZIO:

```scala
libraryDependencies ++= Seq(
  // Cats Effect 3 interop: run queries as F[_] effects and scans as FS2 streams (JVM)
  "dev.zio" %% "zio-dynamodb-ce" % "@VERSION@",

  // Future interop: run queries as Scala Futures, e.g. from a Play application (JVM)
  "dev.zio" %% "zio-dynamodb-future" % "@VERSION@"
)
```

See [How to Use ZIO DynamoDB From Cats Effect](how-to/use-from-cats-effect.md) and [How to Use ZIO DynamoDB From Future-Based Code](how-to/use-from-future-code.md).

### DynamoDB JSON

This experimental module reads and writes the JSON form of DynamoDB items that the AWS CLI and Console use, which helps when debugging data mappings:

```scala
libraryDependencies ++= Seq(
  // DynamoDB JSON: render items and case classes as AWS DynamoDB JSON and parse them back (JVM)
  "dev.zio" %% "zio-dynamodb-json" % "@VERSION@"
)
```

See [ZIO DynamoDB JSON Module](reference/zio-dynamodb-json.md).
