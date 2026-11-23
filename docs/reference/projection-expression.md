---
id: projection-expression
title: "ProjectionExpression"
---

A `ProjectionExpression[-From, +To]` is a path to an attribute inside an item of type `From`, whose value has type `To`. It names attributes in [projection expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html), and it is the starting point for condition, update and key expressions. The High Level and the Low Level API share it, but build it differently.

## Construction

The High Level API generates typed projection expressions from a model's schema with `ProjectionExpression.accessors`:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, group: String, name: String)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, String, Person] = DeriveSchema.gen[Person]

  // id, group and name are ProjectionExpression[Person, String]
  val (id, group, name) = ProjectionExpression.accessors[Person]
}
```

The Low Level API parses a path string with `ProjectionExpression.$`, which returns a `ProjectionExpression[Any, ProjectionExpression.Unknown]`:

```scala mdoc:silent
import zio.dynamodb.ProjectionExpression.$

val id   = $("id")
val name = $("name")
```

See [$ and parse Functions](low-level-api/dollar-function.md) for the path syntax, and [Field Traversal](high-level-api/creating-models/field-traversal.md) for navigating into nested fields, maps and lists with `>>>`, `valueAt` and `elementAt`.

## Type Safety

Typed projection expressions catch many mistakes at compile time. Each of these lines fails to compile:

```scala
Person.id === 1                             // id is a String
Person.id === "1" && Person.name === 2      // name is a String
Person.id === "1" && Employee.name === "2"  // conditions on two different models
```

An untyped projection expression accepts any value, so the same mistake compiles and only fails at runtime:

```scala mdoc:silent
val compilesButWrong = $("name") === 1
```

## API Summary

These methods build the other expression types from a projection expression. Each group has its own reference page:

| Category                         | Methods                                                                                                                                                                   | Reference                                                            |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Primary keys                     | `partitionKey`, `sortKey`                                                                                                                                                 | [Primary Keys](high-level-api/primary-keys.md)                       |
| Condition and filter expressions | `===`, `<>`, `<`, `<=`, `>`, `>=`, `between`, `in`, `inSet`, `beginsWith`, `contains`, `containsSet`, `exists`, `notExists`, `size`, `isBinary` and the other type checks | [Condition and Filter Expressions](condition-expressions.md)         |
| Update actions                   | `set`, `setIfNotExists`, `append`, `appendList`, `prepend`, `prependList`, `add`, `addSet`, `deleteFromSet`, `remove`                                                     | [update](high-level-api/crud-operations/update.md#update-actions)    |
| Traversal                        | `>>>`, `valueAt`, `elementAt`, `some`                                                                                                                                     | [Field Traversal](high-level-api/creating-models/field-traversal.md) |

Every comparison also accepts another projection expression of the same type, which compares two attributes of the same item:

```scala mdoc:silent
val sameGroupAndName: ConditionExpression[Person] = Person.group === Person.name
```

## Converting Between Typed and Untyped Expressions

You rarely need these; they are escape hatches for mixing the two APIs. `ProjectionExpression#unsafeTo[To2]` gives an untyped expression a value type, `ProjectionExpression#unsafeFrom[From2]` gives any expression a different item type, and `ProjectionExpression.$$[From, To](path)` parses a path straight into a typed expression. None of them is checked, so a wrong type fails at runtime:

```scala mdoc:silent
val typedName: ProjectionExpression[Person, String] = $("name").unsafeTo[String].unsafeFrom[Person]
```
