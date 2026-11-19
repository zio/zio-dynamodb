---
id: run-operations-in-a-transaction
title: "How to Run Operations in a Transaction"
sidebar_label: "Run a Transaction"
description: "Make several DynamoDB reads or writes succeed or fail together with DynamoDBQuery#transaction, conditionCheck and safeTransaction."
keywords:
  - "Transactions"
  - "TransactWriteItems"
  - "TransactGetItems"
  - "conditionCheck"
  - "DynamoDBQuery"
---

In this guide, we enrol a student on a course with several writes that all happen or none do, then read related items in one consistent snapshot. By the end, you will know how to turn zipped queries into a [DynamoDB transaction](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html), guard it with conditions, make it idempotent and handle its failures.

## The Problem

Enrolling a student touches three tables: we add the student, add a bill and remove the student from the waiting list. Run as separate queries, a failure halfway leaves a student without a bill, or on the waiting list and enrolled at once. Retrying the whole sequence after a timeout can bill the student twice.

DynamoDB's `TransactWriteItems` and `TransactGetItems` APIs solve this, and in ZIO DynamoDB a transaction is the same zipped query with `DynamoDBQuery#transaction` called on it.

## Prerequisites

This guide only needs the core module:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

The examples use three models, each in its own table keyed by `email`, and a `Course` table keyed by `name`:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.dynamodb.DynamoDBQuery._
import zio.schema.{ DeriveSchema, Schema }

case class Student(email: String, subject: String)
object Student {
  implicit val schema: Schema.CaseClass2[String, String, Student] = DeriveSchema.gen[Student]

  val (email, subject) = ProjectionExpression.accessors[Student]
}

case class Bill(email: String, amount: Int)
object Bill {
  implicit val schema: Schema.CaseClass2[String, Int, Bill] = DeriveSchema.gen[Bill]

  val (email, amount) = ProjectionExpression.accessors[Bill]
}

case class WaitList(email: String)
object WaitList {
  implicit val schema: Schema.CaseClass1[String, WaitList] = DeriveSchema.gen[WaitList]

  val email: ProjectionExpression[WaitList, String] = ProjectionExpression.accessors[WaitList]
}

case class Course(name: String, seats: Int)
object Course {
  implicit val schema: Schema.CaseClass2[String, Int, Course] = DeriveSchema.gen[Course]

  val (name, seats) = ProjectionExpression.accessors[Course]
}
```

## Writing Several Items Atomically

We build the three writes as ordinary queries, zip them, and call `DynamoDBQuery#transaction` on the result. The writes can target different tables:

```scala mdoc:silent
val student = Student("avi@gmail.com", "maths")
val bill    = Bill("avi@gmail.com", 1)

val putStudent         = put("student", student)
val billStudent        = put("billing", bill)
val deleteFromWaitList = deleteFrom("waitlist")(WaitList.email.partitionKey === student.email)

val enrolment = (putStudent zip billStudent zip deleteFromWaitList).transaction
```

Running it sends one `TransactWriteItems` request:

```scala mdoc:compile-only
val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] = enrolment.execute.unit
```

A write transaction returns no items: every `put`, `update` and `deleteFrom` inside it yields `None`, whatever `returns` says.

## Guarding the Writes With Conditions

Conditions set with `DynamoDBQuery#where` on a `put`, `update` or `deleteFrom` inside the transaction are checked as part of it. To check an item we do not write, we add a `DynamoDBQuery.conditionCheck`, which takes a table, a primary key and a condition. Here the enrolment only succeeds while the course has free seats, and the student is only added if not enrolled already:

```scala mdoc:silent
val courseHasSeats = conditionCheck("course", PrimaryKey("name" -> "maths"))(Course.seats > 0)

val guardedEnrolment =
  (
    courseHasSeats
      zip putStudent.where(Student.email.notExists)
      zip billStudent
      zip deleteFromWaitList
      zip update("course")(Course.name.partitionKey === "maths")(Course.seats.add(-1))
  ).transaction
```

`DynamoDBQuery.conditionCheck` only has an effect inside a transaction: executed on its own, it sends no request and succeeds.

## Making a Write Transaction Idempotent

When a transaction times out, we cannot tell whether DynamoDB applied it. `DynamoDBQuery#withClientRequestToken` attaches a [client request token](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken): DynamoDB applies a repeated transaction with the same token only once within ten minutes. The token is set on the transaction, so we call it after `DynamoDBQuery#transaction`:

```scala mdoc:silent
val idempotentEnrolment = guardedEnrolment.withClientRequestToken("enrol-avi-maths")
```

## Reading Several Items Consistently

Zipped `get` queries become one `TransactGetItems` request, which reads all items from the same point in time. The result has the same shape as without the transaction:

```scala mdoc:compile-only
val getStudentAndBill =
  (get("student")(Student.email.partitionKey === "avi@gmail.com") zip
    get("billing")(Bill.email.partitionKey === "avi@gmail.com")).transaction

val program: ZIO[DynamoDBExecutor, DynamoDBError, (Either[DynamoDBError.ItemError, Student], Either[DynamoDBError.ItemError, Bill])] =
  getStudentAndBill.execute
```

## Which Queries a Transaction Accepts

A transaction holds either writes or reads, never both:

| Transaction type                | Accepted queries                                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Write (`TransactWriteItems`)    | `put`/`putItem`, `update`/`updateItem`, `deleteFrom`/`deleteItem`, `DynamoDBQuery.conditionCheck`, and `batch` of them |
| Read (`TransactGetItems`)       | `get`/`getItem`, and `batch` of them                                                                     |

Scans, queries and table operations cannot be part of a transaction. DynamoDB also limits a transaction to 100 actions on distinct items.

## Handling Failures

A transaction can fail in two ways. When the query itself is not a valid transaction, the failure is a `DynamoDBError.TransactionError`: `EmptyTransaction`, `MixedTransactionTypes` (reads and writes together) or `InvalidTransactionActions` (for example a scan). `DynamoDBQuery#safeTransaction` reports these before anything runs, returning an `Either` instead of a query:

```scala mdoc:silent
val invalid = (putStudent zip scanAll[Student]("student")).safeTransaction
```

The scan makes the transaction invalid, so the result is a `Left`:

```scala mdoc
invalid.left.map(_.message)
```

When DynamoDB cancels a valid transaction, for example because a condition is false, the query fails with a `DynamoDBError.AWSError` whose cause is a `TransactionCanceledException`, and none of the writes is applied:

```scala mdoc:compile-only
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

val enrolled: ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  idempotentEnrolment.execute.as(true).catchSome {
    case DynamoDBError.AWSError(_: TransactionCanceledException) => ZIO.succeed(false)
  }
```

## Going Further

- [How to Handle a Failed Conditional Write](handle-failed-conditional-writes.md) covers conditions on single writes.
- [Condition and Filter Expressions](../reference/condition-expressions.md) lists every condition we can use in `DynamoDBQuery#where` and `DynamoDBQuery.conditionCheck`.
- [Error Handling](../reference/error-handling.md) describes the whole `DynamoDBError` hierarchy.
- The integration tests in [TypeSafeApiCrudSpec.scala](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiCrudSpec.scala) and [LiveSpec.scala](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala) run transactions against DynamoDB Local.
