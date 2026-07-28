---
id: index
title: "Introduction to ZIO DynamoDB (series/3.x)"
sidebar_title: "ZIO DynamoDB 3.x"
---

Simple, type-safe, and efficient access to DynamoDB

@PROJECT_BADGES@

## Introduction

This is the `series/3.x` line of ZIO DynamoDB — a from-scratch rewrite built on
`zio-blocks-schema` rather than `zio-schema`. It is currently at an early, hello-world
stage: the codebase and this documentation site are both placeholders while the real
module-by-module import from the rewrite happens.

For the current, production-ready release, see the [`series/2.x`
documentation](https://zio.dev/zio-dynamodb/) instead.

## Installation

To use ZIO DynamoDB 3.x, add the following to your `build.sbt` (not yet published):

```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-dynamodb" % "@VERSION@"
)
```
