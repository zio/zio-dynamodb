---
id: index
title: "High Level API"
sidebar_label: "High Level API"
---

The High Level API constructors live in the `DynamoDBQuery` companion object. They read and write values of our own types, so they need an implicit ZIO `Schema` for the type, usually derived with `DeriveSchema.gen` in the companion object (see [Creating Models](creating-models/index.md)). ZIO DynamoDB derives the codecs between the type and DynamoDB items from that schema. The constructors are:

```scala
object DynamoDBQuery {

  // CRUD operations
  def get[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[From, Either[ItemError, From]]
  def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]]
  def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(action: Action[From]): DynamoDBQuery[From, Option[From]]
  def deleteFrom[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]): DynamoDBQuery[Any, Option[From]]

  // Scan and query operations
  def scanAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
  def scanSome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]
  def queryAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
  def querySome[A: Schema](tableName: String, limit: Int): DynamoDBQuery[A, (Chunk[A], LastEvaluatedKey)]

  // Top-level sum types (experimental)
  def getWithNarrow[From: Schema.Enum, To <: From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[To]): DynamoDBQuery[From, Either[ItemError, To]]
  def putWithNarrow[From: Schema.Enum, To <: From: Schema](tableName: String, a: To): DynamoDBQuery[To, Option[To]]
}
```

The CRUD operations are documented under [CRUD Operations](crud-operations/index.md), the scan and query operations under [Scan and Query Operations](scan-and-query-operations/index.md), and the narrowing operations on the [put](crud-operations/put.md#storing-a-top-level-sum-type) and [get](crud-operations/get.md#reading-a-subtype-of-a-top-level-sum-type) pages.

Operations that take a primary key expect a `KeyConditionExpr.PrimaryKeyExpr[From]`. We do not create one directly; we build it from the model's projection expressions with `partitionKey` and `sortKey`, as described in [Primary Keys](primary-keys.md).
