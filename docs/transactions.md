---
id: transactions
title: "DynamoDB Transactions"
sidebar_label: "Transactions"
---

# DynamoDB Transactions

Transactions are as simple as calling the `.transact` method on a `DynamoDBQuery`. As long as every component of the query is a valid transaction item and the `DyanmoDBQuery` does not have a mix of get and write transaction items. A list of valid items for both types of queries is listed below.

## Examples

### Write Transactions
```scala
final case class Bill(studentEmail: String, semesters: Int)

val student = Student("avi@gmail.com", "maths")
val bill = Bill("avi@gmail.com", 1)

val putStudent = put("student", student)
val billedStudent = put("billing", bill)
val deleteFromWaitlist = deleteItem("waitlist", PrimaryKey("email" -> student.email))

val studentEnrollmentTransaction = (putStudent zip billedStudent zip deleteFromWaitlist).transact

for {
  _ <- studentEnrollmentTransaction.execute
} yield ()
```

### ReadTransactions

```scala
final case class EnrolledClass(courseId: String, studentEmail: String)

val avi = Student("avi@gmail.com", "maths")
val maths101 = EnrolledClass("mth-101", avi.email)
val maths102 = EnrolledClass("mth-102", avi.email)

val putAvi = put("student", avi)
val putClasses = put("enrolledClass", maths101) zip put("enrolledClass", maths102)

val enrollAvi = (putAvi zip putClasses).transaction
```

## Transaction Failures

DynamoDBQueries using the `.transaction` method will fail at runtime if there are invalid transaction actions such as creating a table, scanning for items, or querying. The [DynamoDB documentation] has a limited number of actions that can be performed for either a read or a write transaction. There is a `.safeTransaction` method that is also available that will return `Either[Throwable, DynamoDBQuery[A]]`.

There are more examples in our [integration tests](../dynamodb/src/it/scala/zio/dynamodb/LiveSpec.scala).

### Valid Transact Write Items

* PutItem
* DeleteItem
* BatchWriteItem
* UpdateItem
* ConditionCheck


### Valid Transact Get Item

* GetItem
* BatchGetItem
