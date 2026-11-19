---
id: condition-expressions
title: "Condition and Filter Expressions"
description: "Build type-safe DynamoDB condition and filter expressions from ProjectionExpressions, and know where each is accepted."
keywords:
  - "DynamoDB"
  - "Condition expression"
  - "Filter expression"
  - "ConditionExpression"
---

A `ConditionExpression[-From]` is a **boolean test on the attributes of an item of type `From`**. DynamoDB evaluates it on the server: a condition on a write decides whether the write happens, and a filter on a scan or query decides which items come back. `FilterExpression[From]` is a type alias for `ConditionExpression[From]`; the name only says where the expression goes.

We never construct a `ConditionExpression` directly. We start from a `ProjectionExpression` (a path to an attribute, see [ProjectionExpression](projection-expression.md)) and call a comparison or function on it. The expression then combines with `&&`, `||` and `!`:

```scala
sealed trait ConditionExpression[-From] {
  def &&[From1 <: From](that: ConditionExpression[From1]): ConditionExpression[From1]
  def ||[From1 <: From](that: ConditionExpression[From1]): ConditionExpression[From1]
  def unary_![From1 <: From]: ConditionExpression[From1]
}

type FilterExpression[-From] = ConditionExpression[From]
```

The type parameter `From` is the item type the expression talks about. With the High Level API it is the case class whose accessors built the expression; with the Low Level API's `$` paths it is `Any`.

## Usage

We use the model below throughout this page:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int, minAge: Int, tags: Set[String], nickname: Option[String])
object Person {
  implicit val schema: Schema.CaseClass6[String, String, Int, Int, Set[String], Option[String], Person] =
    DeriveSchema.gen[Person]
  val (id, name, age, minAge, tags, nickname) = ProjectionExpression.accessors[Person]
}
```

A condition on `DynamoDBQuery.put` only writes when it holds, and a filter on `DynamoDBQuery.scanAll` only returns matching items:

```scala mdoc:compile-only
val insertIfNew =
  DynamoDBQuery.put("people", Person("1", "Ann", 30, 18, Set("admin"), None)).where(Person.id.notExists)

val adults =
  DynamoDBQuery.scanAll[Person]("people").filter(Person.age >= 18 && Person.tags.contains("admin"))
```

## Where Conditions Are Accepted

The same expression type serves three purposes, each with its own entry point:

| Entry point                    | Applies to                                                                           | DynamoDB parameter    |
| ------------------------------ | ------------------------------------------------------------------------------------ | --------------------- |
| `DynamoDBQuery#where`          | `DynamoDBQuery.put`, `update`, `deleteFrom` and their `*Item` forms                  | `ConditionExpression` |
| `DynamoDBQuery#filter`         | `DynamoDBQuery.scanAll`, `scanSome`, `queryAll`, `querySome` and their `*Item` forms | `FilterExpression`    |
| `DynamoDBQuery.conditionCheck` | A write transaction                                                                  | `ConditionCheck`      |

A failed write condition fails the effect with `DynamoDBError.AWSError` wrapping the AWS `ConditionalCheckFailedException`; see [How to Handle a Failed Conditional Write](../how-to/handle-failed-conditional-writes.md). A filter runs after DynamoDB reads the items, so it reduces the data sent back but not the read capacity consumed. In a query, a filter cannot use the table's key attributes; put those in the key condition (`DynamoDBQuery#whereKey`, see [Primary Keys](high-level-api/primary-keys.md)).

`DynamoDBQuery.conditionCheck` takes a Low Level API `PrimaryKey` and a condition, and only has an effect inside a write transaction, where a false condition cancels the whole transaction:

```scala
object DynamoDBQuery {
  def conditionCheck(tableName: String, primaryKey: PrimaryKey)(conditionExpression: ConditionExpression[_]): ConditionCheck
}
```

To transfer money only while the sender's account is not frozen:

```scala mdoc:compile-only
import zio.dynamodb.ProjectionExpression.$

val transfer =
  (DynamoDBQuery.conditionCheck("accounts", PrimaryKey("id" -> "alice"))($("frozen") === false) zip
    DynamoDBQuery.updateItem("accounts", PrimaryKey("id" -> "alice"))($("balance").add(-10)) zip
    DynamoDBQuery.updateItem("accounts", PrimaryKey("id" -> "bob"))($("balance").add(10))).transaction
```

Executed outside a transaction, a `DynamoDBQuery.conditionCheck` sends nothing to DynamoDB and returns `None`. See [How to Run Operations in a Transaction](../how-to/run-operations-in-a-transaction.md).

## Comparisons

A comparison tests an attribute against a value or against another attribute of the same item. The value must have the attribute's Scala type, so `Person.age === "30"` does not compile:

```scala
// on ProjectionExpression[From, To]
def ===(that: To): ConditionExpression[From]
def <>(that: To): ConditionExpression[From]
def <(that: To): ConditionExpression[From]
def <=(that: To): ConditionExpression[From]
def >(that: To): ConditionExpression[From]
def >=(that: To): ConditionExpression[From]

def ===(that: ProjectionExpression[From, To]): ConditionExpression[From]
// <>, <, <=, >, >= have the same ProjectionExpression overload

def between(minValue: To, maxValue: To): ConditionExpression[From]
def in(value: To, values: To*): ConditionExpression[From]
def inSet(values: Set[To]): ConditionExpression[From]
```

`between` is inclusive at both ends. `in` and `inSet` hold when the attribute equals any of the values.

To compare an attribute with a value, with another attribute and with a set of values:

```scala mdoc:compile-only
val oldEnough: ConditionExpression[Person]   = Person.age >= Person.minAge
val inTwenties: ConditionExpression[Person]  = Person.age.between(20, 29)
val knownNames: ConditionExpression[Person]  = Person.name.in("Ann", "Bob")
val sameNames: ConditionExpression[Person]   = Person.name.inSet(Set("Ann", "Bob"))
val notCalledBob: ConditionExpression[Person] = Person.name <> "Bob"
```

## Functions

DynamoDB's condition functions are methods on `ProjectionExpression`:

| Method                                                                                                                 | DynamoDB function      | Holds when                                                               |
| ---------------------------------------------------------------------------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------ |
| `exists`                                                                                                               | `attribute_exists`     | The item has the attribute                                               |
| `notExists`                                                                                                            | `attribute_not_exists` | The item lacks the attribute                                             |
| `beginsWith(prefix)`                                                                                                   | `begins_with`          | A string attribute starts with `prefix`                                  |
| `contains(a)`                                                                                                          | `contains`             | A string contains the substring, or a set or list contains the element   |
| `containsSet(head, tail)`                                                                                              | several `contains`     | The attribute contains every element of `tail + head` (joined with `&&`) |
| `isString`, `isNumber`, `isBool`, `isBinary`, `isList`, `isMap`, `isNull`, `isStringSet`, `isNumberSet`, `isBinarySet` | `attribute_type`       | The attribute has that DynamoDB type                                     |
| `size`                                                                                                                 | `size`                 | Not a condition; returns an operand to compare                           |

To insert only new items, look for tagged people and test an optional attribute's type:

```scala mdoc:compile-only
val newItem: ConditionExpression[Person]  = Person.id.notExists
val admins: ConditionExpression[Person]   = Person.tags.contains("admin")
val allRoles: ConditionExpression[Person] = Person.tags.containsSet("admin", Set("owner"))
val aNames: ConditionExpression[Person]   = Person.name.beginsWith("A")
val hasNick: ConditionExpression[Person]  = Person.nickname.exists && Person.nickname.isString
```

`size` returns the length of a string, or the number of elements of a set, list or map, as an operand. We compare it with a number to get a condition:

```scala mdoc:compile-only
val shortName: ConditionExpression[Person] = Person.name.size <= 3
val manyTags: ConditionExpression[Person]  = Person.tags.size > 2
```

## Combining Conditions

`&&`, `||` and `!` combine conditions on the same item type. DynamoDB evaluates them with the usual precedence, and the library adds parentheses around each side when it renders the expression:

```scala mdoc:compile-only
val target: ConditionExpression[Person] =
  (Person.age >= 18 && Person.tags.contains("admin")) || !Person.nickname.exists
```

## Low Level API Conditions

With the Low Level API, `ProjectionExpression.$` builds an untyped path (see [$ and parse Functions](low-level-api/dollar-function.md)). The same methods are available, but they accept any value that has a `ToAttributeValue` instance, so a type mismatch between the attribute and the value is only found by DynamoDB at run time:

```scala mdoc:compile-only
import zio.dynamodb.ProjectionExpression.$

val lowLevel: ConditionExpression[Any] =
  $("age") > 21 && $("name").beginsWith("A") && $("address.city").exists

val deleteIfInactive =
  DynamoDBQuery.deleteItem("people", PrimaryKey("id" -> "1")).where($("active") === false)
```

## Why Some Expressions Do Not Compile

The High Level API checks at compile time that an expression makes sense for the attribute and for the query that runs it. Each check is an implicit evidence type in `zio.dynamodb.proofs`; when it is missing, the compiler reports the message below.

| Evidence      | Required by               | Holds for                                                                                     | Compiler message starts with                                                         |
| ------------- | ------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `CanWhere`    | `DynamoDBQuery#where`     | `DynamoDBQuery.put`, `update` and `deleteFrom` whose item type matches the condition's `From` | "DynamoDB only supports conditions expressions on put, update and delete operations" |
| `CanFilter`   | `DynamoDBQuery#filter`    | Scan and query operations whose item type matches the filter's `From`                         | "DynamoDB only supports filter on scan and query operations"                         |
| `Beginnable`  | `beginsWith`              | `String` and `Option[String]` attributes                                                      | "DynamoDB does not support 'beginsWith' on type"                                     |
| `Containable` | `contains`, `containsSet` | `Set[A]` and `List[A]` with an `A` element, `String` and `Option[String]` with a `String`     | "DynamoDB does not support [...].contains"                                           |
| `Sizable`     | `size`                    | `String` and any `Iterable`                                                                   | "DynamoDB does not support the 'size' operation on type"                             |

For example, `DynamoDBQuery.get("people")(Person.id.partitionKey === "1").where(Person.age > 18)` does not compile, because DynamoDB's `GetItem` takes no condition, and `Person.age.beginsWith("1")` does not compile, because `Person.age` is an `Int`. Paths built with `$` have the type `ProjectionExpression.Unknown`, which every evidence type accepts, so these checks do not apply to the Low Level API.
