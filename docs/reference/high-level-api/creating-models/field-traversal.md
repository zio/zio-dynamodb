---
id: field-traversal
title: "Field Traversal"
---

DynamoDB lets expressions reach into nested structures with [document paths](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html#Expressions.Attributes.NestedElements). The High Level API does the same in a type-safe way: it starts from the projection expressions that `ProjectionExpression.accessors` generates (through ZIO Schema's reified optics) and extends them with traversal combinators.

The examples use a model with a map of lists, `hobbies`, and a nested case class, `address`:

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Address(number: String, street: String)
object Address {
  implicit val schema: Schema.CaseClass2[String, String, Address] = DeriveSchema.gen[Address]

  val (number, street) = ProjectionExpression.accessors[Address]
}

case class Person(email: String, hobbies: Map[String, List[String]], address: Address)
object Person {
  implicit val schema: Schema.CaseClass3[String, Map[String, List[String]], Address, Person] =
    DeriveSchema.gen[Person]

  val (email, hobbies, address) = ProjectionExpression.accessors[Person]
}
```

## Traversal Combinators

Each combinator returns a longer projection expression whose types follow the path:

| Combinator      | Description                                                                                        |
| --------------- | -------------------------------------------------------------------------------------------------- |
| `>>>`           | Continues into a field of a product (case class), or into a case of a sum type (sealed trait).     |
| `valueAt(key)`  | Continues into the value at a string key of a map field.                                           |
| `elementAt(n)`  | Continues into the element at zero-based index `n` of a list field.                                |

The paths below have the types shown, checked by the compiler:

```scala mdoc:silent
val street: ProjectionExpression[Person, String]          = Person.address >>> Address.street
val sports: ProjectionExpression[Person, List[String]]    = Person.hobbies.valueAt("sports")
val firstSport: ProjectionExpression[Person, String]      = Person.hobbies.valueAt("sports").elementAt(0)
```

The `ProjectionExpression#toString` of a path is its DynamoDB document path:

```scala mdoc
firstSport.toString
```

Every expression built from a path works on the nested value. To update nested fields of one item:

```scala mdoc:compile-only
val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    _ <- DynamoDBQuery
           .put("people", Person("john@gmail.com", Map("sports" -> List("cricket", "football")), Address("1", "Main St")))
           .execute
    _ <- DynamoDBQuery
           .update("people")(Person.email.partitionKey === "john@gmail.com")(
             street.set("2nd St") + sports.set(List("tennis", "rugby"))
           )
           .execute
    _ <- DynamoDBQuery
           .update("people")(Person.email.partitionKey === "john@gmail.com")(firstSport.set("cricket"))
           .execute
  } yield ()
```

## Sum Type Traversal

For a sealed trait, `ProjectionExpression.accessors` returns one projection expression per subtype, and `>>>` continues from a field of the sealed trait's type into one subtype. The model below stores a `BilledBody` sum type inside `Invoice`:

```scala mdoc:silent
import zio.schema.annotation.discriminatorName

@discriminatorName("billedType")
sealed trait BilledBody
object BilledBody {
  case class BilledMonthly(month: Int) extends BilledBody
  object BilledMonthly {
    implicit val schema: Schema.CaseClass1[Int, BilledMonthly] = DeriveSchema.gen[BilledMonthly]

    val month: ProjectionExpression[BilledMonthly, Int] = ProjectionExpression.accessors[BilledMonthly]
  }

  case class BilledYearly(year: Int) extends BilledBody
  object BilledYearly {
    implicit val schema: Schema.CaseClass1[Int, BilledYearly] = DeriveSchema.gen[BilledYearly]

    val year: ProjectionExpression[BilledYearly, Int] = ProjectionExpression.accessors[BilledYearly]
  }

  implicit val schema: Schema.Enum2[BilledMonthly, BilledYearly, BilledBody] = DeriveSchema.gen[BilledBody]

  val (monthly, yearly) = ProjectionExpression.accessors[BilledBody]
}

case class Invoice(id: Int, body: BilledBody)
object Invoice {
  implicit val schema: Schema.CaseClass2[Int, BilledBody, Invoice] = DeriveSchema.gen[Invoice]

  val (id, body) = ProjectionExpression.accessors[Invoice]
}
```

The path from an invoice to the `year` of a yearly bill goes through the `yearly` case. With `@discriminatorName`, the subtype's fields sit next to the discriminator field inside `body`, so the document path is `body.year`:

```scala mdoc
val yearPath: ProjectionExpression[Invoice, Int] = Invoice.body >>> BilledBody.yearly >>> BilledBody.BilledYearly.year

yearPath.toString
```

The path then works like any other:

```scala mdoc:compile-only
val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    _ <- DynamoDBQuery.put("invoices", Invoice(1, BilledBody.BilledYearly(2021))).execute
    _ <- DynamoDBQuery.update("invoices")(Invoice.id.partitionKey === 1)(yearPath.set(2022)).execute
  } yield ()
```

## Optional Fields

`ProjectionExpression.some` continues from an `Option` field into its value, so conditions and updates can use the value's type: `Person.nickName >>> ProjectionExpression.some[String]` is a `ProjectionExpression[Person, String]` for a field `nickName: Option[String]`.
