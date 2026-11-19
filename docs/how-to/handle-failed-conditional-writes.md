---
id: handle-failed-conditional-writes
title: "How to Handle a Failed Conditional Write"
sidebar_label: "Handle Failed Conditional Writes"
description: "Guard put, update and delete with a condition and turn DynamoDB's ConditionalCheckFailedException into a domain result."
keywords:
  - "Conditional Write"
  - "Optimistic Locking"
  - "ConditionalCheckFailedException"
  - "DynamoDBError"
  - "where"
---

In this guide, we write to DynamoDB only when a condition holds and react when it does not. By the end, you will have an insert that refuses to overwrite an existing account, an update protected by a version number (optimistic locking), and a small domain error type that tells "the condition failed" apart from real storage failures.

We start with an insert guarded by `ProjectionExpression#notExists`, recognise the failure it produces, map it to a domain error, add a versioned update, and finish with what changes when conditional writes are combined or batched.

## The Problem

A plain `put` replaces whatever item has the same key, so two clients that create the same account race and the last one wins silently. DynamoDB solves this with condition expressions: the write happens only if the condition is true, and otherwise the request fails with a `ConditionalCheckFailedException`.

That failure is an expected outcome, not an outage, but it travels through the same error channel as network errors and throttling. With the plain AWS SDK, the caller writes the condition as a string, wires up placeholder names and catches the exception by type:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, ConditionalCheckFailedException, PutItemRequest }
import scala.jdk.CollectionConverters._

def createAccount(client: DynamoDbClient, id: String, owner: String): Boolean = {
  val item    = Map(
    "id"    -> AttributeValue.builder().s(id).build(),
    "owner" -> AttributeValue.builder().s(owner).build()
  ).asJava
  val request = PutItemRequest
    .builder()
    .tableName("accounts")
    .item(item)
    .conditionExpression("attribute_not_exists(#id)") // a typo here fails only at run time
    .expressionAttributeNames(Map("#id" -> "id").asJava)
    .build()
  try {
    client.putItem(request)
    true
  } catch {
    case _: ConditionalCheckFailedException => false
  }
}
```

With ZIO DynamoDB, the condition is a typed expression built from the model's accessors, and the failure arrives as a value we can pattern match on.

## Prerequisites

Conditional writes are part of the core module:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The examples need a DynamoDB with an `accounts` table whose partition key is `id`; see [How to Connect to DynamoDB Local](connect-to-dynamodb-local.md). They share these imports and a model of a bank account that carries a version number:

```scala mdoc:silent
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery.{ put, update }
import zio.schema.{ DeriveSchema, Schema }

case class Account(id: String, owner: String, balance: Long, version: Int)
object Account {
  implicit val schema: Schema.CaseClass4[String, String, Long, Int, Account] = DeriveSchema.gen[Account]

  val (id, owner, balance, version) = ProjectionExpression.accessors[Account]
}
```

## Inserting Only When the Item Is New

`DynamoDBQuery#where` attaches a condition expression to a `put`, `update` or `deleteFrom`. `ProjectionExpression#notExists` is true when the item has no such attribute; because every stored item has its partition key, `Account.id.notExists` means "no item with this key exists yet".

To insert an account without overwriting an existing one:

```scala mdoc:silent
def insert(account: Account): ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  put("accounts", account)
    .where(Account.id.notExists)
    .execute
    .unit
```

The condition is checked against the item stored under `account.id`. The compiler rejects a condition built from another model's accessors, and it rejects `DynamoDBQuery#where` on queries that DynamoDB does not allow it on, such as `DynamoDBQuery.get` or `DynamoDBQuery.scanAll`.

## Recognising the Failure

When the condition is false, `execute` fails with `DynamoDBError.AWSError`, whose `cause` is the AWS SDK exception. For a failed condition that cause is a `ConditionalCheckFailedException`, so the pattern to match is `DynamoDBError.AWSError(_: ConditionalCheckFailedException)`. Every other AWS failure, such as throttling or a missing table, is also an `AWSError`, with a different cause.

To report "already exists" as `false` instead of an error:

```scala mdoc:silent
def insertIfNew(account: Account): ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  insert(account)
    .as(true)
    .catchSome { case DynamoDBError.AWSError(_: ConditionalCheckFailedException) =>
      ZIO.succeed(false)
    }
```

`ZIO#catchSome` handles only the matched case; all other errors stay in the error channel unchanged.

## Mapping to a Domain Error

Callers usually want to know why a write did not happen without seeing AWS types. A small error type keeps the expected outcomes apart from infrastructure failures.

To define the error type and a helper that classifies a `DynamoDBError`:

```scala mdoc:silent
sealed trait AccountError
object AccountError {
  case class AlreadyExists(id: String)                  extends AccountError
  case class VersionConflict(id: String, expected: Int) extends AccountError
  case class Storage(cause: DynamoDBError)              extends AccountError
}

def classify(onConditionFailed: => AccountError)(error: DynamoDBError): AccountError =
  error match {
    case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => onConditionFailed
    case other                                                      => AccountError.Storage(other)
  }
```

To open an account with the domain error type:

```scala mdoc:silent
def open(account: Account): ZIO[DynamoDBExecutor, AccountError, Unit] =
  insert(account).mapError(classify(AccountError.AlreadyExists(account.id)))
```

## Updating Only the Version You Read

Optimistic locking stores a version number in each item. A writer reads the item, changes it, and writes it back only if the version is still the one it read, incrementing it in the same update. If another writer got there first, the condition fails and the caller can reload and retry.

To change the balance only if the stored version matches:

```scala mdoc:silent
def setBalance(id: String, newBalance: Long, expectedVersion: Int): ZIO[DynamoDBExecutor, AccountError, Unit] =
  update("accounts")(Account.id.partitionKey === id)(
    Account.balance.set(newBalance) + Account.version.set(expectedVersion + 1)
  ).where(Account.version === expectedVersion)
    .execute
    .unit
    .mapError(classify(AccountError.VersionConflict(id, expectedVersion)))
```

Conditions combine with `&&`, `||` and `!`. To also refuse an update that would touch a missing item, extend the condition to `Account.id.exists && Account.version === expectedVersion`; without it, `update` creates the item when none exists.

## Combining and Batching Conditional Writes

Conditional writes cannot go into a `BatchWriteItem`, because DynamoDB's batch API does not accept condition expressions. This changes how combined writes run:

- In writes combined with `DynamoDBQuery#zip`, each write that has a condition is sent as its own request, in parallel with the rest. Each one succeeds or fails on its own, and the combined query fails with the first error.
- Writes combined with `DynamoDBQuery.batch` fail with `DynamoDBError.BatchError.UnbatchableQueryError` when two or more of them have a condition, before anything is sent. Its message names the reason, `PutItem has a condition expression`.
- Writes combined with `DynamoDBQuery#transaction` succeed or fail together. A failed condition cancels the whole transaction, and the cause inside `AWSError` is a `TransactionCanceledException` instead of a `ConditionalCheckFailedException`.

For many independent conditional writes, run each one as its own query, for example with `ZIO.foreachPar(accounts)(insertIfNew)`. For writes that must all happen or none, use a transaction, described in [How to Run Operations in a Transaction](run-operations-in-a-transaction.md).

## Going Further

- [ProjectionExpression](../reference/projection-expression.md) lists every operator available in conditions, such as `exists`, `between`, `beginsWith` and `size`.
- [Error Handling](../reference/error-handling.md) describes the full `DynamoDBError` hierarchy.
- [put](../reference/high-level-api/crud-operations/put.md) and [update](../reference/high-level-api/crud-operations/update.md) list the other combinators these queries accept, such as `returns`.
