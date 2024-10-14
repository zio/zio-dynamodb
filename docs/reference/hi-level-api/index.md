---
id: index
title: "High Level API"
sidebar_label: "High Level API"
---

High Level API methods are found on the `DynamoDBQuery` companion object. They rely on a ZIO Schema for a particular type being in implicit scope. This is achieved using the `DeriveSchema.gen` macro. Internally codecs are automatically generated for the case classes based on the meta data provided by the `Schema`'s.

```scala
object DynamoDBQuery {

  def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]] = ???

  def get[From: Schema](tableName: String)(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]
  ): DynamoDBQuery[From, Either[ItemError, From]] = ???

  def update[From: Schema](tableName: String)(primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From])(
      action: Action[From]
    ): DynamoDBQuery[From, Option[From]]  = ???

  def deleteFrom[From: Schema](
    tableName: String
  )(
    primaryKeyExpr: KeyConditionExpr.PrimaryKeyExpr[From]
  ): DynamoDBQuery[Any, Option[From]] = ???

}
```

