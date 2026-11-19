---
id: model-data-within-schema-limits
title: "How to Model Data Within ZIO Schema's Limits"
sidebar_label: "Model Data Within Schema Limits"
description: "Shape case classes and sealed traits so the High Level API can derive codecs and type-safe projection expressions for every field."
keywords:
  - "Data Modelling"
  - "ZIO Schema"
  - "Sum Types"
  - "Product Types"
  - "ProjectionExpression"
---

In this guide, we shape a domain model so that the High Level API can store it and build a type-safe expression for every field we query or update. By the end, you will have an invoice model whose common fields are reachable from the top, with no field-count or inheritance problems.

We look at the two limits the High Level API inherits from ZIO Schema, then refactor an inheritance-based model into product and sum types.

## The Problem

The High Level API relies on ZIO Schema twice: to derive the codecs, and to generate projection expressions with `ProjectionExpression.accessors`. Two kinds of model defeat the second step:

- **Case classes with more than 22 fields.** ZIO Schema derives a typed `Schema.CaseClassN` only up to 22 fields. Above that, it derives a generic record schema. ZIO DynamoDB still stores and reads such a class, but `ProjectionExpression.accessors` no longer returns one typed projection expression per field, so we cannot write conditions, keys or updates for its fields.
- **Deep inheritance hierarchies.** ZIO Schema derives a sealed trait as a flat enum of its concrete case classes. An intermediate sealed trait, and abstract members declared on the traits, get no projection expression of their own.

For example, an object-oriented invoice model puts `id` on the root trait and `amount` on an intermediate trait:

```scala mdoc:silent
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

object OopStyle {
  sealed trait Invoice {
    def id: Int
  }
  // An intermediate abstraction with an abstract field
  sealed trait Billed extends Invoice {
    def amount: Double
  }
  case class BilledMonthly(id: Int, amount: Double, month: Int) extends Billed
  case class BilledYearly(id: Int, amount: Double, year: Int)   extends Billed
  case class PreBilled(id: Int, count: Int)                     extends Invoice

  object Invoice {
    implicit val schema: Schema.Enum3[BilledMonthly, BilledYearly, PreBilled, Invoice] = DeriveSchema.gen[Invoice]

    val (billedMonthly, billedYearly, preBilled) = ProjectionExpression.accessors[Invoice]
  }
}
```

The derived schema has three cases, one per concrete class: `Billed` has disappeared, and there is no `Invoice.id` or `Invoice.amount` projection expression. To find invoice `1`, every query would have to go through a specific case, and the partition key `id` of a table of invoices cannot be expressed at all.

## Prerequisites

This guide only needs the core module:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

It assumes you know how models, schemas and accessors fit together; see [High Level API](../concepts/high-level-api.md).

## Keeping Case Classes Small

When a case class grows past 22 fields, group related fields into nested case classes. Each nested class has its own schema and accessors, and `>>>` reaches into it (see [Field Traversal](../reference/high-level-api/creating-models/field-traversal.md)):

```scala mdoc:silent
case class Address(street: String, city: String, postcode: String)
object Address {
  implicit val schema: Schema.CaseClass3[String, String, String, Address] = DeriveSchema.gen[Address]

  val (street, city, postcode) = ProjectionExpression.accessors[Address]
}

case class Customer(id: String, name: String, address: Address)
object Customer {
  implicit val schema: Schema.CaseClass3[String, String, Address, Customer] = DeriveSchema.gen[Customer]

  val (id, name, address) = ProjectionExpression.accessors[Customer]
}

val customerCity: ProjectionExpression[Customer, String] = Customer.address >>> Address.city
```

Nested case classes are stored as DynamoDB maps, so the path `address.city` exists in the stored item too.

## Replacing Inheritance With Product and Sum Types

To fix the invoice model, we move fields up into case classes instead of declaring them on traits. Each fix is an "extract product" refactoring:

1. The fields every invoice has (`id`) move into a top-level `Invoice` case class, which holds the varying part in a `body` field.
2. The fields every billed invoice has (`amount`) move into a `Billed` case class, which holds the billing period in its own sum type.

The resulting model uses no inheritance beyond one level of sealed trait:

```scala mdoc:silent
object FpStyle {
  sealed trait BilledBody
  case class BilledMonthly(month: Int) extends BilledBody
  case class BilledYearly(year: Int)   extends BilledBody
  object BilledBody {
    implicit val schema: Schema.Enum2[BilledMonthly, BilledYearly, BilledBody] = DeriveSchema.gen[BilledBody]

    val (monthly, yearly) = ProjectionExpression.accessors[BilledBody]
  }

  sealed trait InvoiceBody
  case class Billed(amount: Double, billedBody: BilledBody) extends InvoiceBody
  case class PreBilled(count: Int)                          extends InvoiceBody
  object Billed {
    implicit val schema: Schema.CaseClass2[Double, BilledBody, Billed] = DeriveSchema.gen[Billed]

    val (amount, billedBody) = ProjectionExpression.accessors[Billed]
  }
  object InvoiceBody {
    implicit val schema: Schema.Enum2[Billed, PreBilled, InvoiceBody] = DeriveSchema.gen[InvoiceBody]

    val (billed, preBilled) = ProjectionExpression.accessors[InvoiceBody]
  }

  case class Invoice(id: Int, body: InvoiceBody)
  object Invoice {
    implicit val schema: Schema.CaseClass2[Int, InvoiceBody, Invoice] = DeriveSchema.gen[Invoice]

    val (id, body) = ProjectionExpression.accessors[Invoice]
  }
}
```

Every field now has a typed projection expression reachable from `Invoice`: `Invoice.id` is the partition key, and the amount of a billed invoice is a path through the `body` field and the `billed` case:

```scala mdoc:silent
import FpStyle._

val billedAmount: ProjectionExpression[Invoice, Double] = Invoice.body >>> InvoiceBody.billed >>> Billed.amount
```

## Querying and Updating the Refactored Model

With these expressions, the usual High Level API calls work on the whole hierarchy. To store an invoice, then raise the amount of invoice `1`:

```scala mdoc:compile-only
import zio._

val invoice = Invoice(1, Billed(10.0, BilledMonthly(11)))

val program: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  for {
    _ <- DynamoDBQuery.put("invoices", invoice).execute
    _ <- DynamoDBQuery.update("invoices")(Invoice.id.partitionKey === 1)(billedAmount.set(20.0)).execute
  } yield ()
```

The refactoring also makes each concrete class smaller, which keeps it far from the 22-field limit.

## Going Further

- [How to Customize How Sum Types Are Stored](customize-sum-type-encoding.md) changes how the `body` field's cases are stored.
- [Field Traversal](../reference/high-level-api/creating-models/field-traversal.md) documents `>>>`, `valueAt` and `elementAt`.
- The integration test [TypeSafeApiAlternateModeling.scala](https://github.com/zio/zio-dynamodb/blob/series/2.x/dynamodb/src/it/scala/zio/dynamodb/TypeSafeApiAlternateModeling.scala) runs this model against DynamoDB Local.
