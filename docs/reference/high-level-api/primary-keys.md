---
id: primary-keys
title: "Primary Keys"
---

The AWS DynamoDB API represents primary keys in two different ways, depending on the operation:

| AWS                       | Example                         | Used by                                                                                                                                                  |
| ------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Primary key               | `{"id": "1", "year": 2023}`     | `GetItem`, `UpdateItem`, `DeleteItem` ([AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-Key)) |
| Key condition expression  | `#id = :v1 and #year > :v2`     | `Query` ([AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.KeyConditionExpressions.html))                                  |

A primary key is a JSON object whose values are matched for equality, while a key condition is an expression that can also compare the sort key with `<`, `between` and so on. The High Level API unifies both into one type-safe API.

## Building Key Expressions

We build key expressions from the model's projection expressions with `partitionKey` and `sortKey`. The examples use two models, both with `id` as partition key; `Person` has the numeric sort key `year` and `Employee` the string sort key `group`:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, year: Int, address: String)
object Person {
  implicit val schema: Schema.CaseClass3[String, Int, String, Person] = DeriveSchema.gen[Person]

  val (id, year, address) = ProjectionExpression.accessors[Person]
}

case class Employee(id: String, group: String, address: String)
object Employee {
  implicit val schema: Schema.CaseClass3[String, String, String, Employee] = DeriveSchema.gen[Employee]

  val (id, group, address) = ProjectionExpression.accessors[Employee]
}
```

An equality on the partition key, optionally followed by `&&` and an equality on the sort key, is a `KeyConditionExpr.PrimaryKeyExpr`. The single-item operations (`get`, `update`, `deleteFrom`) take this type:

```scala mdoc:silent
val personKey: KeyConditionExpr.PrimaryKeyExpr[Person] =
  Person.id.partitionKey === "1" && Person.year.sortKey === 2020
```

A partition key equality followed by any sort key condition is a `KeyConditionExpr`, which queries accept in `DynamoDBQuery#whereKey`:

```scala mdoc:silent
val recentPeople: KeyConditionExpr[Person] =
  Person.id.partitionKey === "1" && Person.year.sortKey > 2020

val firstGroups: KeyConditionExpr[Employee] =
  Employee.id.partitionKey === "1" && Employee.group.sortKey.beginsWith("Group1")
```

Passing a key condition with `>` to `get` is a compile error, because it is not a `PrimaryKeyExpr`.

## Valid Operations

These operations build key expressions:

| On                                              | Operation                       | Accepted by                                                     |
| ----------------------------------------------- | ------------------------------- | --------------------------------------------------------------- |
| `Person.id.partitionKey`, `Person.year.sortKey` | `===`                           | `get`, `update`, `deleteFrom` and `whereKey`                    |
| `Person.id.partitionKey === "1"`                | `&&` (with a sort key condition) | `get`, `update`, `deleteFrom` (sort key `===` only) and `whereKey` |
| `Person.year.sortKey`                           | `>`, `>=`, `<`, `<=`             | `whereKey` only                                                 |
|                                                 | `between(2021, 2023)`           | `whereKey` only                                                 |
| `Employee.group.sortKey`                        | `beginsWith("Group1")`          | `whereKey` only; string and binary sort keys only               |

`partitionKey` and `sortKey` are only available on fields whose type DynamoDB allows as a key: `String`, the numeric types (`Short`, `Int`, `Long`, `Float`, `Double`, `BigDecimal`), binary data, and `Option`s of these.

:::warning
`SortKey` also has a `<>` operator, which compiles but is not a valid key condition operator in DynamoDB; a query that uses it fails at runtime. Use a filter (`DynamoDBQuery#filter`) for "not equal" conditions on the sort key.
:::

## Low Level Keys

The Low Level API builds the same expressions from untyped projection expressions, for example `$("id").partitionKey === "1" && $("year").sortKey > 2020`. A `PrimaryKey` (an `AttrMap`) such as `PrimaryKey("id" -> "1", "year" -> 2020)` takes the place of `PrimaryKeyExpr` in `getItem`, `updateItem` and `deleteItem`.
