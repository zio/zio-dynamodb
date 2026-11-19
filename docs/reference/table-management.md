---
id: table-management
title: "Table Management"
description: "Create, describe and delete DynamoDB tables, including key schemas, billing modes and secondary indexes."
keywords:
  - "DynamoDB"
  - "Create table"
  - "Secondary index"
  - "KeySchema"
  - "BillingMode"
---

Table management queries **create, inspect and drop DynamoDB tables** from Scala. They are ordinary [`DynamoDBQuery`](dynamodb-query.md) values built by `DynamoDBQuery.createTable`, `DynamoDBQuery.describeTable` and `DynamoDBQuery.deleteTable`, so we run them with `DynamoDBQuery#execute` like any other query. Applications usually create tables with infrastructure tools; these queries are most useful in tests, examples and local development against [DynamoDB Local](../how-to/connect-to-dynamodb-local.md).

The constructors live on the `DynamoDBQuery` companion object:

```scala
object DynamoDBQuery {
  def createTable(
    tableName: String,
    keySchema: KeySchema,
    billingMode: BillingMode,
    sseSpecification: Option[SSESpecification] = None,
    tags: Map[String, String] = Map.empty
  )(attributeDefinition: AttributeDefinition, attributeDefinitions: AttributeDefinition*): CreateTable

  def describeTable(tableName: String): DescribeTable // a DynamoDBQuery[Any, DescribeTableResponse]

  def deleteTable(tableName: String): DeleteTable     // a DynamoDBQuery[Any, Unit]
}
```

`CreateTable` is a `DynamoDBQuery[Any, Unit]`. Its own type is internal, so annotate a stored query as `DynamoDBQuery[Any, Unit]`.

## Usage

To create a table keyed by a string `id`, run something against it and drop it again:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val createPeople: DynamoDBQuery[Any, Unit] =
  DynamoDBQuery.createTable("people", KeySchema("id"), BillingMode.PayPerRequest)(
    AttributeDefinition.attrDefnString("id")
  )

val program: ZIO[DynamoDBExecutor, DynamoDBError, DynamoDBQuery.DescribeTableResponse] =
  for {
    _        <- createPeople.execute
    response <- DynamoDBQuery.describeTable("people").execute
    _        <- DynamoDBQuery.deleteTable("people").execute
  } yield response
```

## Creating Tables

A `DynamoDBQuery.createTable` call names the table, its primary key, how it is billed and the type of every key attribute. Optional arguments set server-side encryption and tags.

### Key Schema

`KeySchema` names the partition key (DynamoDB's "hash key") and, optionally, the sort key ("range key"):

```scala
final case class KeySchema private (hashKey: String, sortKey: Option[String])

object KeySchema {
  def apply(hashKey: String): KeySchema
  def apply(hashKey: String, sortKey: String): KeySchema
}
```

The names must match the attribute names the items use. With the High Level API those are the case class field names (or the `@fieldName` annotation values).

### Attribute Definitions

DynamoDB needs the type of every attribute that is part of a key, either of the table or of a secondary index. The second parameter list of `DynamoDBQuery.createTable` takes one or more `AttributeDefinition`s:

```scala
object AttributeDefinition {
  def attrDefnString(name: String): AttributeDefinition
  def attrDefnNumber(name: String): AttributeDefinition
  def attrDefnBinary(name: String): AttributeDefinition
}
```

Define only key attributes here; DynamoDB rejects definitions for attributes that no key uses.

### Billing Mode

`BillingMode` chooses between on-demand and provisioned capacity:

```scala
sealed trait BillingMode

object BillingMode {
  case object PayPerRequest extends BillingMode
  def provisioned(readCapacityUnit: Long, writeCapacityUnit: Long): BillingMode // the concrete Provisioned type is internal
}
```

`BillingMode.PayPerRequest` bills per request and needs no capacity numbers. `BillingMode.provisioned` sets the table's read and write capacity units.

To create a table with a composite key (partition key `customerId`, sort key `orderId`) and provisioned capacity:

```scala mdoc:compile-only
import zio.dynamodb._

val createOrders: DynamoDBQuery[Any, Unit] =
  DynamoDBQuery.createTable(
    "orders",
    KeySchema("customerId", "orderId"),
    BillingMode.provisioned(readCapacityUnit = 5, writeCapacityUnit = 5)
  )(
    AttributeDefinition.attrDefnString("customerId"),
    AttributeDefinition.attrDefnString("orderId")
  )
```

### Encryption and Tags

`DynamoDBQuery.createTable` also accepts an `SSESpecification` for server-side encryption and a map of tags (DynamoDB allows up to 50 tags per table):

```scala
final case class SSESpecification(
  enable: Boolean = false,
  kmsMasterKeyId: Option[String] = None,
  sseType: SSESpecification.SSEType
)

object SSESpecification {
  sealed trait SSEType
  case object AES256 extends SSEType
  case object KMS    extends SSEType
}
```

To encrypt a table with a customer-managed KMS key and tag it:

```scala mdoc:compile-only
import zio.dynamodb._

val createSecrets: DynamoDBQuery[Any, Unit] =
  DynamoDBQuery.createTable(
    "secrets",
    KeySchema("id"),
    BillingMode.PayPerRequest,
    sseSpecification = Some(
      SSESpecification(enable = true, kmsMasterKeyId = Some("alias/my-key"), sseType = SSESpecification.KMS)
    ),
    tags = Map("team" -> "payments")
  )(AttributeDefinition.attrDefnString("id"))
```

## Secondary Indexes

A secondary index lets a query use a different key than the table's primary key. We add indexes to a `DynamoDBQuery.createTable` query with `DynamoDBQuery#gsi` and `DynamoDBQuery#lsi`, and later query them with `DynamoDBQuery#indexName`. Calling these methods on any other kind of query leaves it unchanged. Their signatures:

```scala
trait DynamoDBQuery[-In, +Out] {
  def gsi(indexName: String, keySchema: KeySchema, projection: ProjectionType): DynamoDBQuery[In, Out]
  def gsi(
    indexName: String,
    keySchema: KeySchema,
    projection: ProjectionType,
    readCapacityUnit: Long,
    writeCapacityUnit: Long
  ): DynamoDBQuery[In, Out]
  def lsi(indexName: String, keySchema: KeySchema, projection: ProjectionType = ProjectionType.All): DynamoDBQuery[In, Out]
}
```

A global secondary index (`DynamoDBQuery#gsi`) can use any attributes as its keys. On a `BillingMode.provisioned` table, use the overload with capacity units; on a `BillingMode.PayPerRequest` table, use the one without. A local secondary index (`DynamoDBQuery#lsi`) keeps the table's partition key and adds a different sort key; DynamoDB only accepts it as part of table creation.

The `projection` argument decides which attributes DynamoDB copies into the index:

```scala
sealed trait ProjectionType

object ProjectionType {
  case object KeysOnly extends ProjectionType
  final case class Include private (nonKeyAttributes: NonEmptySet[String]) extends ProjectionType
  object Include {
    def apply(nonKeyAttribute: String, nonKeyAttributes: String*): Include
  }
  case object All extends ProjectionType
}
```

| Projection                     | Attributes in the index                                 |
| ------------------------------ | ------------------------------------------------------- |
| `ProjectionType.KeysOnly`      | Table and index keys only                               |
| `ProjectionType.Include(a, b)` | Keys plus the named attributes (DynamoDB allows up to 20) |
| `ProjectionType.All`           | Every attribute of the item                             |

To create an orders table with a global index on `status` and a local index on `createdAt`, we define every key attribute the indexes use too:

```scala mdoc:compile-only
import zio.dynamodb._

val createOrders: DynamoDBQuery[Any, Unit] =
  DynamoDBQuery
    .createTable("orders", KeySchema("customerId", "orderId"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("customerId"),
      AttributeDefinition.attrDefnString("orderId"),
      AttributeDefinition.attrDefnString("status"),
      AttributeDefinition.attrDefnNumber("createdAt")
    )
    .gsi("byStatus", KeySchema("status", "orderId"), ProjectionType.Include("total"))
    .lsi("byCreatedAt", KeySchema("customerId", "createdAt"), ProjectionType.KeysOnly)
```

## Describing Tables

`DynamoDBQuery.describeTable` returns a summary of an existing table:

```scala
object DynamoDBQuery {
  final case class DescribeTableResponse(
    tableArn: String,
    tableStatus: TableStatus,
    tableSizeBytes: Long,
    itemCount: Long
  )

  sealed trait TableStatus
  object TableStatus {
    case object Creating                          extends TableStatus
    case object Updating                          extends TableStatus
    case object Deleting                          extends TableStatus
    case object Active                            extends TableStatus
    case object InaccessibleEncryptionCredentials extends TableStatus
    case object Archiving                         extends TableStatus
    case object Archived                          extends TableStatus
    case object ReplicationNotAuthorized          extends TableStatus
    case object unknownToSdkVersion               extends TableStatus
  }
}
```

DynamoDB updates `tableSizeBytes` and `itemCount` about every six hours, so they are estimates. `TableStatus.unknownToSdkVersion` appears when AWS returns a status the SDK does not know yet.

To wait until a new table is ready, we can poll its status:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.TableStatus

val waitUntilActive: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  DynamoDBQuery
    .describeTable("orders")
    .execute
    .repeatUntil(_.tableStatus == TableStatus.Active)
    .unit
```

## Deleting Tables

`DynamoDBQuery.deleteTable` drops a table and all its items. In tests, pairing `DynamoDBQuery.createTable` and `DynamoDBQuery.deleteTable` in `ZIO.acquireRelease` gives every run a fresh table:

```scala mdoc:compile-only
import zio._
import zio.dynamodb._

val peopleTable: ZLayer[DynamoDBExecutor, DynamoDBError, Unit] =
  ZLayer.scoped(
    ZIO.acquireRelease(
      DynamoDBQuery
        .createTable("people", KeySchema("id"), BillingMode.PayPerRequest)(AttributeDefinition.attrDefnString("id"))
        .execute
    )(_ => DynamoDBQuery.deleteTable("people").execute.orDie)
  )
```

The in-memory [test executor](test-executor.md) does not support `DynamoDBQuery.createTable`, `DynamoDBQuery.describeTable` or `DynamoDBQuery.deleteTable`; we create tables there with `TestDynamoDBExecutor.addTable`.
