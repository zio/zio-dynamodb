---
id: index
title: "Introduction to ZIO DynamoDB (series/3.x)"
sidebar_title: "ZIO DynamoDB 3.x"
---

Simple, type-safe, and efficient access to DynamoDB

@PROJECT_BADGES@

## Introduction

`series/3.x` is a major overhaul of ZIO DynamoDB 2.x: a highly modular library with a
zero-dependency core, plus a high level API built on [ZIO Blocks](https://zio.dev/zio-blocks).
It's under active, module-by-module development — modules are being migrated in one at a
time, so not everything from 2.x is available here yet.

For the current, production-ready release, see the [`series/2.x`
documentation](https://zio.dev/zio-dynamodb/) instead.

## Installation

To use ZIO DynamoDB 3.x, add the following to your `build.sbt` (not yet published):

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb" % "@SNAPSHOT_VERSION@"
)
```
