---
id: put
title: "put"
---

`DynamoDBQuery.put` inserts an item, or replaces the item that has the same primary key:

```scala
object DynamoDBQuery {
  def put[A: Schema](tableName: String, a: A): DynamoDBQuery[A, Option[A]]
}
```

The examples use this model:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Person(id: String, name: String, age: Int)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, Int, Person] = DeriveSchema.gen[Person]

  val (id, name, age) = ProjectionExpression.accessors[Person]
}
```

## Inserting Only New Items

`DynamoDBQuery.put` replaces an existing item silently. To insert only when no item has the key yet, add a condition with `DynamoDBQuery#where`; when the condition is false the query fails with `DynamoDBError.AWSError` wrapping a `ConditionalCheckFailedException` (see [How to Handle a Failed Conditional Write](../../../how-to/handle-failed-conditional-writes.md)):

```scala mdoc:compile-only
val insert: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery
    .put("person", Person("1", "John", 21))
    .where(Person.id.notExists)
    .execute
```

## Options

These options apply to `DynamoDBQuery.put`:

```scala
<PUT_QUERY>
  .returns(<ReturnValues>)       // ReturnValues.AllOld | ReturnValues.None (default)
  .where(<ConditionExpression>)  // e.g. Person.id.notExists
  .metrics(<ReturnItemCollectionMetrics>)
  .capacity(<ReturnConsumedCapacity>)
```

The result is `None` unless `DynamoDBQuery#returns(ReturnValues.AllOld)` is set, in which case it holds the item that was replaced, if any:

```scala mdoc:compile-only
val replaced: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
  DynamoDBQuery.put("person", Person("1", "John", 22)).returns(ReturnValues.AllOld).execute
```

## Storing a Top-Level Sum Type

When a table stores the subtypes of a sealed trait annotated with `@discriminatorName`, the discriminator field is only written if the value is encoded with the sealed trait's schema. `put[Invoice]` writes it; `put` inferred as `put[Unpaid]` uses `Unpaid`'s schema and does not:

```scala mdoc:silent
import zio.schema.annotation.discriminatorName
import zio.dynamodb.json._

@discriminatorName("invoiceType")
sealed trait Invoice {
  def id: String
}
object Invoice {
  case class Unpaid(id: String) extends Invoice
  object Unpaid {
    implicit val schema: Schema.CaseClass1[String, Unpaid] = DeriveSchema.gen[Unpaid]

    val id = ProjectionExpression.accessors[Unpaid]
  }
  case class Paid(id: String, amount: Int) extends Invoice
  object Paid {
    implicit val schema: Schema.CaseClass2[String, Int, Paid] = DeriveSchema.gen[Paid]

    val (id, amount) = ProjectionExpression.accessors[Paid]
  }

  implicit val schema: Schema.Enum2[Unpaid, Paid, Invoice] = DeriveSchema.gen[Invoice]
}
```

The [ZIO DynamoDB JSON](../../zio-dynamodb-json.md) module shows the difference between the two encodings:

```scala mdoc
println(Invoice.Unpaid("1").toJsonString[Invoice])
println(Invoice.Unpaid("1").toJsonString[Invoice.Unpaid])
```

So `DynamoDBQuery.put[Invoice]("invoice", Invoice.Unpaid("1"))` stores the discriminator and `DynamoDBQuery.put("invoice", Invoice.Unpaid("1"))` does not. The experimental `DynamoDBQuery.putWithNarrow[From, To]` does both: it encodes with the sealed trait's schema, so the discriminator is stored, and returns a query typed by the subtype, so conditions can use the subtype's fields:

```scala mdoc:compile-only
val putUnpaid: ZIO[DynamoDBExecutor, DynamoDBError, Option[Invoice.Unpaid]] =
  DynamoDBQuery
    .putWithNarrow[Invoice, Invoice.Unpaid]("invoice", Invoice.Unpaid("1"))
    .where(!Invoice.Unpaid.id.exists)
    .execute
```

The matching read is [`getWithNarrow`](get.md#reading-a-subtype-of-a-top-level-sum-type).
