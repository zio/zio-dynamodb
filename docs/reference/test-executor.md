---
id: test-executor
title: "Test Executor"
description: "DynamoDBExecutor.test: an in-memory DynamoDBExecutor for fast unit tests, with TestDynamoDBExecutor to seed tables and inspect queries."
keywords:
  - "Testing"
  - "ZIO Test"
  - "In-memory DynamoDB"
  - "TestDynamoDBExecutor"
---

`DynamoDBExecutor.test` is an **in-memory `DynamoDBExecutor` for unit tests**. It keeps each table as a map from primary key to item, so tests run without DynamoDB, Docker or network access. Alongside the executor, the layer provides the `TestDynamoDBExecutor` service, which seeds tables with items, lists a table's items and records every query the code under test ran.

The fake understands only the basics: it stores and returns items by partition key and ignores expressions. Use it to test code that reads and writes whole items; for anything that depends on DynamoDB's own behaviour (conditions, updates, key conditions, filters, indexes, transactions), test against DynamoDB Local instead, as [How to Test Code That Uses DynamoDB](../how-to/test-dynamodb-code.md) describes.

The service and the layers that provide it:

```scala
trait TestDynamoDBExecutor {
  def addTable(tableName: String, pkFieldName: String, pkAndItems: (PrimaryKey, Item)*): UIO[Unit]
  def addItems(tableName: String, pkAndItems: (PrimaryKey, Item)*): ZIO[Any, DynamoDBError, Unit]
  def recordedQueries: UIO[List[DynamoDBQuery[_, _]]]
  def itemsForTable(tableName: String): UIO[Set[Item]]
}

object DynamoDBExecutor {
  lazy val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor]
  def test(tableAndPKNames: (String, String)*): ULayer[DynamoDBExecutor with TestDynamoDBExecutor]
}
```

## Usage

We use this model in the examples:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
  val (id, name) = ProjectionExpression.accessors[Person]
}
```

To run a put and a get against an empty in-memory `people` table whose partition key is `id`:

```scala mdoc:silent
val roundTrip: ZIO[DynamoDBExecutor, DynamoDBError, Either[DynamoDBError.ItemError, Person]] =
  for {
    _      <- DynamoDBQuery.put("people", Person("1", "Ann")).execute
    person <- DynamoDBQuery.get("people")(Person.id.partitionKey === "1").execute
  } yield person
```

Running it with the test layer returns the stored person:

```scala mdoc
Unsafe.unsafe { implicit unsafe =>
  Runtime.default.unsafe.run(roundTrip.provide(DynamoDBExecutor.test("people" -> "id"))).getOrThrowFiberFailure()
}
```

## Creating the Layer

A table must exist in the fake before any query touches it; a query on an unknown table fails with `DynamoDBError.ItemError.ValueNotFound("table ... does not exist")`. There are two ways to create tables.

`DynamoDBExecutor.test(tableAndPKNames: _*)` creates empty tables up front. Each pair is a table name and the name of its partition key attribute:

```scala mdoc:compile-only
val layer: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] =
  DynamoDBExecutor.test("people" -> "id", "orders" -> "orderId")
```

`DynamoDBExecutor.test` without arguments starts with no tables. We then create each table with `TestDynamoDBExecutor.addTable`, optionally with initial items given as Low Level API `(PrimaryKey, Item)` pairs:

```scala mdoc:compile-only
val seeded: ZIO[TestDynamoDBExecutor, Nothing, Unit] =
  TestDynamoDBExecutor.addTable(
    "people",
    "id",
    PrimaryKey("id" -> "1") -> Item("id" -> "1", "name" -> "Ann"),
    PrimaryKey("id" -> "2") -> Item("id" -> "2", "name" -> "Bob")
  )
```

The companion object `TestDynamoDBExecutor` has accessor functions for every method of the service, so tests call them directly as `ZIO` effects that need `TestDynamoDBExecutor` in the environment.

## Seeding and Inspecting Data

The service methods fall into seeding (`TestDynamoDBExecutor#addTable`, `TestDynamoDBExecutor#addItems`) and inspection (`TestDynamoDBExecutor#itemsForTable`, `TestDynamoDBExecutor#recordedQueries`).

`TestDynamoDBExecutor.addItems` adds items to an existing table and fails with `ValueNotFound` if the table does not exist. `TestDynamoDBExecutor.itemsForTable` returns every item of a table as a set (empty for an unknown table). `TestDynamoDBExecutor.recordedQueries` returns, in order, every query the executor received. Composite queries arrive split into their parts, and two or more gets or writes arrive as one batch request.

To seed a table, run the code under test and check the stored items and the queries that ran:

```scala mdoc:silent
val checked: ZIO[DynamoDBExecutor with TestDynamoDBExecutor, DynamoDBError, (Set[Item], Int)] =
  for {
    _       <- TestDynamoDBExecutor.addTable("people", "id")
    _       <- TestDynamoDBExecutor.addItems("people", PrimaryKey("id" -> "1") -> Item("id" -> "1", "name" -> "Ann"))
    _       <- DynamoDBQuery.deleteFrom[Person]("people")(Person.id.partitionKey === "1").execute
    items   <- TestDynamoDBExecutor.itemsForTable("people")
    queries <- TestDynamoDBExecutor.recordedQueries
  } yield (items, queries.size)
```

After the delete, the table is empty and the executor recorded one query:

```scala mdoc
Unsafe.unsafe { implicit unsafe =>
  Runtime.default.unsafe.run(checked.provide(DynamoDBExecutor.test)).getOrThrowFiberFailure()
}
```

## Supported Operations

The fake implements a subset of DynamoDB. The table below lists each operation and how closely the fake follows DynamoDB:

| Operation                                                                               | Behaviour in the fake                                                                                                                  |
| --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `DynamoDBQuery.get`, `DynamoDBQuery.getItem`                                            | Looks the item up by its exact primary key. Projections are ignored.                                                                   |
| `DynamoDBQuery.put`, `DynamoDBQuery.putItem`                                            | Stores the item under its partition key value. Conditions and return values are ignored; the result is always `None`.                  |
| `DynamoDBQuery.deleteFrom`, `DynamoDBQuery.deleteItem`                                  | Removes the item. Conditions and return values are ignored; the result is always `None`.                                               |
| Batched gets and writes (`zip`, `batch`, streaming helpers)                             | Run item by item, with no unprocessed items.                                                                                           |
| `DynamoDBQuery.scanAll`, `DynamoDBQuery.scanSome`                                       | Return all items, ordered by partition key, paged by `limit` and `startKey`. Filters, projections, indexes and `parallel` are ignored. |
| `DynamoDBQuery.queryAll`, `DynamoDBQuery.querySome`                                     | Behave like a scan: the key condition and filter are ignored and every item of the table comes back.                                   |
| `DynamoDBQuery.update`, `DynamoDBQuery.updateItem`                                      | Not supported.                                                                                                                         |
| `DynamoDBQuery.createTable`, `DynamoDBQuery.deleteTable`, `DynamoDBQuery.describeTable` | Not supported; use `TestDynamoDBExecutor#addTable`.                                                                                    |
| `DynamoDBQuery.conditionCheck`, `transaction`                                           | Not supported.                                                                                                                         |

An unsupported operation makes the fiber die with an exception whose message is `Constructor ... not implemented yet`; it is a defect, not a `DynamoDBError`.

:::warning
The fake keys every table on its partition key only. A `DynamoDBQuery.put` stores the item under a key made of the partition key attribute alone, so a `DynamoDBQuery.get` with a composite primary key (partition key and sort key) never finds it.
:::

## Using It With ZIO Test

A test body is an ordinary effect that needs a `DynamoDBExecutor`. To check that a saved person loads back unchanged:

```scala mdoc:compile-only
val savesAndLoads: ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  for {
    _     <- DynamoDBQuery.put("people", Person("1", "Ann")).execute
    found <- DynamoDBQuery.get("people")(Person.id.partitionKey === "1").execute.absolve
  } yield found == Person("1", "Ann")
```

In a ZIO Test spec, the same `for` comprehension ends in `assertTrue(found == Person("1", "Ann"))` inside `test("saves and loads a person") { ... }`, and the suite gets the in-memory database with `suite("PeopleRepository")(...).provide(DynamoDBExecutor.test("people" -> "id"))`. `Spec#provide` builds a fresh layer for each test, so every test starts with empty tables. To share one in-memory database across the tests of a suite, use `Spec#provideShared` instead.
