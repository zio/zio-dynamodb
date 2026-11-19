---
id: dollar-function
title: "$ and parse Functions"
---

`ProjectionExpression.$` and `ProjectionExpression.parse` create a `ProjectionExpression` from a string in the [AWS DynamoDB document path syntax](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html). They are the main way to build projection expressions in the Low Level API.

## Parsing a Path

`ProjectionExpression.parse` returns a `Left` with an error message when the path is invalid, or a `Right` with the projection expression:

```scala mdoc
import zio.dynamodb.ProjectionExpression

ProjectionExpression.parse("hobbies.sport[0]")
ProjectionExpression.parse("hobbies.sp$ort")
```

`ProjectionExpression.$` is the unsafe version: it returns the projection expression directly and throws an `IllegalStateException` when the path is invalid. A projection expression's `toString` renders it back as a path:

```scala mdoc
import zio.dynamodb.ProjectionExpression.$

$("hobbies.sport[0]").toString
```

Both functions return an untyped projection expression (its value type is `ProjectionExpression.Unknown`), so any value can be compared with it or assigned to it.

## Path Syntax

The parser supports this subset of the DynamoDB path syntax:

| Expression syntax | Description                                                                 |
| ----------------- | --------------------------------------------------------------------------- |
| `"foo"`           | A top-level attribute.                                                      |
| `"foos[0]"`       | An element of a top-level list; indexes start at zero.                      |
| `"foo.bar"`       | An element of a map inside a top-level attribute.                           |
| `"foo_bar"`       | Underscores are valid in names.                                             |
| `"foo-bar"`       | Hyphens are valid in names.                                                 |
| ``"`foo.bar`"``   | Other special characters, such as a dot, need backticks around the name.   |

When a query runs, every path element is replaced by an [expression attribute name](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html) and every value by an [expression attribute value](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeValues.html), so names that clash with DynamoDB reserved words need no escaping.

## Extending a Path

An untyped projection expression can be extended with `apply`: a `String` argument selects a map element and an `Int` argument selects a list element:

```scala mdoc
$("hobbies")("sport")(0).toString
```
