---
id: build-an-order-store
title: "Tutorial: Build an Order Store"
sidebar_label: "Tutorial"
description: "Build a small order store with ZIO DynamoDB step by step: models, tables, CRUD, queries and a transaction on DynamoDB Local."
keywords:
  - "DynamoDB"
  - "Tutorial"
  - "Order Store"
  - "DynamoDBQuery"
  - "DynamoDB Local"
---

Welcome! This tutorial is for Scala developers who know basic ZIO (`ZIO`, `ZLayer`, for-comprehensions) and want to store data in Amazon DynamoDB. You don't need any prior experience with ZIO DynamoDB, and you don't need an AWS account: everything runs on DynamoDB Local, a DynamoDB emulator in a Docker container.

## Learning Objectives

By the end of this tutorial, you will understand:

- How a Scala case class becomes a DynamoDB item through a ZIO Schema and `ProjectionExpression.accessors`
- How a `DynamoDBQuery` describes a request and how `DynamoDBExecutor` runs it
- How to create tables, write, read, update and delete items with the High Level API
- How to query a customer's orders by key, page through them and use a secondary index
- How to change two tables atomically with a transaction

We'll learn these concepts by building a small order store, step by step:

1. Set Up the Project
2. Model the Data
3. Connect to DynamoDB Local
4. Create the Tables
5. Write and Read Items
6. Update an Item
7. Query a Customer's Orders
8. Place an Order in a Transaction
9. Delete Items and Drop the Tables
10. Run the Same Code Against an In-Memory Executor

We recommend reading from top to bottom: each section builds on the previous one.

## Background

The order store keeps two tables. The `products` table holds what we sell, keyed by a product code (the SKU). The `orders` table holds what customers bought, keyed by the customer id plus an order id, so that all orders of one customer sit together and come back sorted by order id.

ZIO DynamoDB separates *describing* a request from *running* it. Every operation in this tutorial first builds a `DynamoDBQuery` value: a plain, immutable description such as "put this product into `products`". Nothing touches the network until we call `DynamoDBQuery#execute`, which turns the description into a ZIO effect that needs a `DynamoDBExecutor` in its environment. Because the executor is just a `ZLayer`, the same queries run against DynamoDB Local, real AWS or an in-memory fake, depending on which layer we provide.

## 1. Set Up the Project

ZIO DynamoDB 2.x needs Java 17 or newer, sbt and, for this tutorial, Docker to run DynamoDB Local. The repository ships a Compose file that starts DynamoDB Local in memory on port 8000; save it as `docker-compose.yml` in your project:

```yaml
services:
  dynamodb:
    image: amazon/dynamodb-local
    container_name: dynamodb-local
    ports:
      - 8000:8000
    command: "-jar DynamoDBLocal.jar -inMemory -sharedDb"
```

Start the container before running any code from this tutorial:

```bash
docker compose -f docker-compose.yml up -d
```

Then add the library to `build.sbt`; it brings ZIO, ZIO Schema and the ZIO AWS DynamoDB client with it:

```scala
libraryDependencies += "dev.zio" %% "zio-dynamodb" % "@VERSION@"
```

ZIO DynamoDB is published for Scala 2.13 and Scala 3. The code in this tutorial uses Scala 2.13 syntax.

## 2. Model the Data

The High Level API stores case classes directly. Each model needs two things in its companion object: an implicit ZIO `Schema`, which ZIO DynamoDB uses to derive the codec between the case class and a DynamoDB item, and the result of `ProjectionExpression.accessors`, which gives us one typed *projection expression* (a path to an attribute) per field. We use those paths to build keys, conditions and updates.

Let's define a `Product` and an `Order`:

```scala mdoc:silent
import zio._
import zio.dynamodb.{ DynamoDBError, DynamoDBExecutor, DynamoDBQuery, LastEvaluatedKey, ProjectionExpression }
import zio.dynamodb.syntax._
import zio.schema.{ DeriveSchema, Schema }

case class Product(sku: String, name: String, priceInCents: Int, stock: Int)
object Product {
  implicit val schema: Schema.CaseClass4[String, String, Int, Int, Product] = DeriveSchema.gen[Product]
  val (sku, name, priceInCents, stock)                                      = ProjectionExpression.accessors[Product]
}

case class Order(customerId: String, orderId: String, sku: String, quantity: Int)
object Order {
  implicit val schema: Schema.CaseClass4[String, String, String, Int, Order] = DeriveSchema.gen[Order]
  val (customerId, orderId, sku, quantity)                                    = ProjectionExpression.accessors[Order]
}
```

The code above:

- `DeriveSchema.gen[Product]` derives the schema at compile time; the explicit `Schema.CaseClass4[...]` type is what `ProjectionExpression.accessors` needs to see the four fields.
- `ProjectionExpression.accessors[Product]` returns a tuple with one `ProjectionExpression[Product, A]` per field, in field order, so `Product.stock` is a `ProjectionExpression[Product, Int]`.
- The `zio.dynamodb.syntax._` import adds the `maybeFound` extension method we use when reading items.

Notice that the paths are typed: `Product.stock` knows it belongs to `Product` and holds an `Int`, so later the compiler rejects a condition such as comparing `Product.stock` with a `String`.

## 3. Connect to DynamoDB Local

`DynamoDBExecutor.live` is the layer that runs queries. It needs the ZIO AWS `DynamoDb` client, which in turn needs an HTTP client and AWS settings. For DynamoDB Local we point the client at `http://localhost:8000` and give it dummy credentials, since the emulator accepts any.

Let's build the layers:

```scala mdoc:silent
import java.net.URI
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import zio.aws.core.config.{ AwsConfig, CommonAwsConfig }
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient

val localAwsSettings: ULayer[CommonAwsConfig] = ZLayer.succeed(
  CommonAwsConfig(
    region = None,
    credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
    endpointOverride = None,
    commonClientConfig = None
  )
)

val dynamoDbLocal: ZLayer[Any, Throwable, DynamoDb] =
  (NettyHttpClient.default ++ localAwsSettings) >>> AwsConfig.default >>> DynamoDb.customized { builder =>
    builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
  }

val localExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] =
  dynamoDbLocal >>> DynamoDBExecutor.live
```

The code above:

- `localAwsSettings` provides static `dummy`/`dummy` credentials instead of looking them up in the environment.
- `NettyHttpClient.default` is the HTTP client the AWS SDK uses; `AwsConfig.default` combines it with our settings.
- `DynamoDb.customized` gives us the AWS SDK client builder, where `endpointOverride` sends every request to DynamoDB Local and `region` sets the region the SDK signs requests for.
- `DynamoDBExecutor.live` turns the `DynamoDb` client into the `DynamoDBExecutor` that `DynamoDBQuery#execute` needs.

Building these layers does not open a connection yet; ZIO builds them when a program that needs them runs.

## 4. Create the Tables

A DynamoDB table has a *primary key*: a partition key alone, or a partition key plus a sort key. `DynamoDBQuery.createTable` takes the table name, the key schema, the billing mode and a definition for every key attribute. Only key attributes need a definition; the other attributes of an item are free-form.

Let's describe both tables:

```scala mdoc:silent
import zio.dynamodb.{ AttributeDefinition, BillingMode, KeySchema, ProjectionType }

val createProducts =
  DynamoDBQuery.createTable("products", KeySchema("sku"), BillingMode.PayPerRequest)(
    AttributeDefinition.attrDefnString("sku")
  )

val createOrders =
  DynamoDBQuery
    .createTable("orders", KeySchema("customerId", "orderId"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("customerId"),
      AttributeDefinition.attrDefnString("orderId"),
      AttributeDefinition.attrDefnString("sku")
    )
    .gsi("orders-by-sku", KeySchema("sku", "orderId"), ProjectionType.All)

val createTables: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  (createProducts zip createOrders).execute.unit
```

The code above:

- `KeySchema("sku")` makes the `"sku"` attribute the partition key of `products`; `KeySchema("customerId", "orderId")` gives `orders` a partition key and a sort key.
- `BillingMode.PayPerRequest` means we pay per request instead of reserving capacity, which also suits DynamoDB Local.
- `AttributeDefinition.attrDefnString` declares each key attribute as a DynamoDB string. The `"sku"` attribute is declared on `orders` too, because the secondary index uses it.
- `DynamoDBQuery#gsi` adds a *global secondary index* named `orders-by-sku`, keyed by the `"sku"` and `"orderId"` attributes, so we can later look up orders by product. `ProjectionType.All` copies every attribute into the index.
- `zip` combines both descriptions into one query, and `DynamoDBQuery#execute` turns that into a ZIO effect. ZIO DynamoDB runs the two independent table creations in parallel.

Notice that `createTables` is still only a value of type `ZIO[DynamoDBExecutor, DynamoDBError, Unit]`: it says what to do and which service it needs.

## 5. Write and Read Items

`DynamoDBQuery.put` writes a whole item, replacing any item with the same key. `DynamoDBQuery.get` reads one item by its primary key. We build the key from the projection expressions of section 2: `Product.sku.partitionKey === "sku-1"` means "the item whose partition key `"sku"` equals `sku-1`".

Let's stock two products and read one back:

```scala mdoc:silent
val keyboard = Product("sku-1", "Keyboard", 4999, 10)
val mouse    = Product("sku-2", "Mouse", 1999, 25)

val addProducts: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  (DynamoDBQuery.put("products", keyboard) zip DynamoDBQuery.put("products", mouse)).execute.unit

val findKeyboard: ZIO[DynamoDBExecutor, DynamoDBError, Option[Product]] =
  DynamoDBQuery.get("products")(Product.sku.partitionKey === "sku-1").execute.maybeFound
```

The code above:

- `DynamoDBQuery.put("products", keyboard)` uses the `Product` schema to encode the case class into an item.
- Zipping two puts lets ZIO DynamoDB send them together in a single `BatchWriteItem` request instead of two round trips.
- `DynamoDBQuery.get` returns `Either[ItemError, Product]` on success: a `Left` when the item is missing or cannot be decoded.
- `maybeFound` turns "not found" into `None` and moves decoding errors into the ZIO error channel, so `findKeyboard` succeeds with `Some(keyboard)` or `None`.

Once the tables exist, running `addProducts *> findKeyboard` against DynamoDB Local returns `Some(Product(sku-1,Keyboard,4999,10))`. We run the whole program in [Putting It Together](#putting-it-together).

## 6. Update an Item

`DynamoDBQuery.update` changes some attributes of an existing item without rewriting it. The changes are *update actions* built from the projection expressions: `set` replaces a value, `add` adds to a number, and `+` combines several actions into one update.

Let's restock the keyboard and lower its price in one request:

```scala mdoc:silent
import zio.dynamodb.ReturnValues

val restockKeyboard: ZIO[DynamoDBExecutor, DynamoDBError, Option[Product]] =
  DynamoDBQuery
    .update("products")(Product.sku.partitionKey === "sku-1")(
      Product.stock.add(5) + Product.priceInCents.set(4499)
    )
    .returns(ReturnValues.AllNew)
    .execute
```

The code above:

- `Product.stock.add(5)` increases the stored stock by 5 on the server, so we never read the old value first.
- `Product.priceInCents.set(4499)` overwrites the price; `+` sends both actions in the same `UpdateItem` request.
- `DynamoDBQuery#returns` with `ReturnValues.AllNew` asks DynamoDB to send back the item as it is after the update. Without it the result is `None`.

After `addProducts`, running `restockKeyboard` returns `Some(Product(sku-1,Keyboard,4499,15))`.

## 7. Query a Customer's Orders

`DynamoDBQuery.get` needs the full primary key. To read *all* orders of one customer we use a query: `DynamoDBQuery.queryAll` takes a *key condition* through `DynamoDBQuery#whereKey`, which fixes the partition key and can narrow the sort key, and it streams every matching item, fetching more pages from DynamoDB as the stream is consumed.

Let's read Alice's orders from November 2026 (order ids start with the date):

```scala mdoc:silent
val alicesNovemberOrders: ZIO[DynamoDBExecutor, Throwable, Chunk[Order]] =
  for {
    stream <- DynamoDBQuery
                .queryAll[Order]("orders")
                .whereKey(Order.customerId.partitionKey === "alice" && Order.orderId.sortKey.beginsWith("2026-11"))
                .execute
    orders <- stream.runCollect
  } yield orders
```

The code above:

- `Order.customerId.partitionKey === "alice"` selects Alice's partition; `&&` adds a condition on the sort key.
- `Order.orderId.sortKey.beginsWith("2026-11")` keeps orders whose id starts with `2026-11`; DynamoDB returns them sorted by the `"orderId"` sort key.
- `DynamoDBQuery#execute` returns a ZIO `Stream`, and `runCollect` gathers it into a `Chunk`.

A web page usually shows one page of results at a time instead of all of them. `DynamoDBQuery.querySome` reads at most `limit` items and also returns a `LastEvaluatedKey`: `Some(key)` when more items may follow, `None` at the end. Passing that key to `DynamoDBQuery#startKey` continues where the previous page stopped:

```scala mdoc:silent
def ordersPage(
  customerId: String,
  startKey: LastEvaluatedKey
): ZIO[DynamoDBExecutor, DynamoDBError, (Chunk[Order], LastEvaluatedKey)] =
  DynamoDBQuery
    .querySome[Order]("orders", limit = 2)
    .whereKey(Order.customerId.partitionKey === customerId)
    .startKey(startKey)
    .execute
```

Calling `ordersPage("alice", None)` returns the first two orders and a key; calling it again with that key returns the next two.

The secondary index from section 4 answers a different question: which orders contain a given product? `DynamoDBQuery#indexName` runs the query against the index, so the key condition uses the index's partition key, `Order.sku`:

```scala mdoc:silent
val keyboardOrders: ZIO[DynamoDBExecutor, Throwable, Chunk[Order]] =
  DynamoDBQuery
    .queryAll[Order]("orders")
    .indexName("orders-by-sku")
    .whereKey(Order.sku.partitionKey === "sku-1")
    .execute
    .flatMap(_.runCollect)
```

Notice that `Order.sku.partitionKey` is only a partition key *in the index*; ZIO DynamoDB does not check which key belongs to which index, so the index name and the key condition must match.

## 8. Place an Order in a Transaction

Placing an order touches two tables: it saves the order and takes the ordered quantity out of stock. If only one of the two writes succeeded, the store would sell stock it doesn't have, or lose an order. A transaction makes both writes succeed or fail together. We also add *condition expressions* with `DynamoDBQuery#where`: the order must not exist yet, and the product must have enough stock.

Let's write `placeOrder`:

```scala mdoc:silent
def placeOrder(order: Order): ZIO[DynamoDBExecutor, DynamoDBError, Unit] = {
  val saveOrder =
    DynamoDBQuery
      .put("orders", order)
      .where(Order.orderId.notExists)

  val takeStock =
    DynamoDBQuery
      .update("products")(Product.sku.partitionKey === order.sku)(Product.stock.add(-order.quantity))
      .where(Product.stock >= order.quantity)

  (saveOrder zip takeStock).transaction.execute.unit
}
```

The code above:

- `Order.orderId.notExists` makes the put fail if an order with this key is already stored, so placing the same order twice doesn't count twice.
- `Product.stock.add(-order.quantity)` decreases the stock, and `Product.stock >= order.quantity` makes the update fail if there is not enough left.
- `zip` combines the two writes, and `DynamoDBQuery#transaction` sends them as one `TransactWriteItems` request instead of separate calls.

When a condition fails, DynamoDB cancels the whole transaction: no order is saved and the stock is unchanged. ZIO DynamoDB reports it as a `DynamoDBError.AWSError` that wraps the AWS SDK's `TransactionCanceledException`, which we can turn into a business result:

```scala mdoc:silent
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException

def tryPlaceOrder(order: Order): ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
  placeOrder(order).as(true).catchSome { case DynamoDBError.AWSError(_: TransactionCanceledException) =>
    ZIO.succeed(false)
  }
```

With 15 keyboards in stock, `tryPlaceOrder(Order("alice", "2026-11-01-001", "sku-1", 2))` returns `true`, and an order for 100 keyboards returns `false` and changes nothing.

## 9. Delete Items and Drop the Tables

`DynamoDBQuery.deleteFrom` removes one item by its full primary key. For `orders` the key has two parts, so we combine the partition key and the sort key with `&&`. `DynamoDBQuery.deleteTable` removes a whole table.

Let's cancel an order and clean up:

```scala mdoc:silent
val cancelOrder: ZIO[DynamoDBExecutor, DynamoDBError, Option[Order]] =
  DynamoDBQuery
    .deleteFrom("orders")(Order.customerId.partitionKey === "alice" && Order.orderId.sortKey === "2026-11-01-001")
    .execute

val dropTables: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
  (DynamoDBQuery.deleteTable("orders") zip DynamoDBQuery.deleteTable("products")).execute.unit
```

The code above:

- `Order.customerId.partitionKey === "alice" && Order.orderId.sortKey === "2026-11-01-001"` names exactly one order; with a composite key, `DynamoDBQuery.deleteFrom` and `DynamoDBQuery.get` always need both parts.
- `dropTables` deletes both tables in parallel. DynamoDB Local runs in memory, so stopping the container drops them too, but deleting them keeps each run of the program independent.

## 10. Run the Same Code Against an In-Memory Executor

So far every effect has needed a `DynamoDBExecutor` without saying which one. `DynamoDBExecutor.test` is an in-memory executor for unit tests: it supports puts, gets, deletes and batches on tables with a partition key, but not updates, conditions, key conditions or transactions. That is enough for the `products` code from section 5.

Let's run `addProducts` and `findKeyboard` unchanged, providing the in-memory executor with a `products` table keyed by the `"sku"` attribute:

```scala mdoc
Unsafe.unsafe { implicit unsafe =>
  Runtime.default.unsafe
    .run((addProducts *> findKeyboard).provide(DynamoDBExecutor.test("products" -> "sku")))
    .getOrThrowFiberFailure()
}
```

The code above runs the program for real, inside this page: `DynamoDBExecutor.test("products" -> "sku")` creates an empty in-memory `products` table whose partition key is the `"sku"` attribute, and the output is the keyboard we stored.

This is the payoff of separating description from execution: our store code never mentions DynamoDB Local, so swapping one layer moves it between a real database and an in-memory fake.

## Putting It Together

The program below combines every step into one runnable application. Unlike the sections, it runs the steps in order inside one `ZIOAppDefault`, creates the tables at the start, drops them at the end and prints each result:

```scala mdoc:compile-only
import java.net.URI
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import zio._
import zio.aws.core.config.{ AwsConfig, CommonAwsConfig }
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb._
import zio.dynamodb.syntax._
import zio.schema.{ DeriveSchema, Schema }

object OrderStoreApp extends ZIOAppDefault {

  case class Product(sku: String, name: String, priceInCents: Int, stock: Int)
  object Product {
    implicit val schema: Schema.CaseClass4[String, String, Int, Int, Product] = DeriveSchema.gen[Product]
    val (sku, name, priceInCents, stock)                                      = ProjectionExpression.accessors[Product]
  }

  case class Order(customerId: String, orderId: String, sku: String, quantity: Int)
  object Order {
    implicit val schema: Schema.CaseClass4[String, String, String, Int, Order] = DeriveSchema.gen[Order]
    val (customerId, orderId, sku, quantity)                                    = ProjectionExpression.accessors[Order]
  }

  val localExecutor: ZLayer[Any, Throwable, DynamoDBExecutor] = {
    val settings = ZLayer.succeed(
      CommonAwsConfig(
        region = None,
        credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")),
        endpointOverride = None,
        commonClientConfig = None
      )
    )
    (NettyHttpClient.default ++ settings) >>> AwsConfig.default >>> DynamoDb.customized { builder =>
      builder.endpointOverride(URI.create("http://localhost:8000")).region(Region.US_EAST_1)
    } >>> DynamoDBExecutor.live
  }

  val createTables: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
    (DynamoDBQuery.createTable("products", KeySchema("sku"), BillingMode.PayPerRequest)(
      AttributeDefinition.attrDefnString("sku")
    ) zip
      DynamoDBQuery
        .createTable("orders", KeySchema("customerId", "orderId"), BillingMode.PayPerRequest)(
          AttributeDefinition.attrDefnString("customerId"),
          AttributeDefinition.attrDefnString("orderId"),
          AttributeDefinition.attrDefnString("sku")
        )
        .gsi("orders-by-sku", KeySchema("sku", "orderId"), ProjectionType.All)).execute.unit

  val dropTables: ZIO[DynamoDBExecutor, DynamoDBError, Unit] =
    (DynamoDBQuery.deleteTable("orders") zip DynamoDBQuery.deleteTable("products")).execute.unit

  def placeOrder(order: Order): ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
    (DynamoDBQuery.put("orders", order).where(Order.orderId.notExists) zip
      DynamoDBQuery
        .update("products")(Product.sku.partitionKey === order.sku)(Product.stock.add(-order.quantity))
        .where(Product.stock >= order.quantity)).transaction.execute
      .as(true)
      .catchSome { case DynamoDBError.AWSError(_: TransactionCanceledException) => ZIO.succeed(false) }

  def ordersPage(customerId: String, startKey: LastEvaluatedKey) =
    DynamoDBQuery
      .querySome[Order]("orders", limit = 2)
      .whereKey(Order.customerId.partitionKey === customerId)
      .startKey(startKey)
      .execute

  val program: ZIO[DynamoDBExecutor, Throwable, Unit] =
    for {
      _          <- createTables
      _          <- (DynamoDBQuery.put("products", Product("sku-1", "Keyboard", 4999, 10)) zip
                      DynamoDBQuery.put("products", Product("sku-2", "Mouse", 1999, 25))).execute
      keyboard   <- DynamoDBQuery.get("products")(Product.sku.partitionKey === "sku-1").execute.maybeFound
      _          <- Console.printLine(s"Found: $keyboard")
      restocked  <- DynamoDBQuery
                      .update("products")(Product.sku.partitionKey === "sku-1")(
                        Product.stock.add(5) + Product.priceInCents.set(4499)
                      )
                      .returns(ReturnValues.AllNew)
                      .execute
      _          <- Console.printLine(s"Restocked: $restocked")
      placed1    <- placeOrder(Order("alice", "2026-11-01-001", "sku-1", 2))
      placed2    <- placeOrder(Order("alice", "2026-11-02-001", "sku-2", 1))
      placed3    <- placeOrder(Order("alice", "2026-11-03-001", "sku-2", 3))
      tooMany    <- placeOrder(Order("bob", "2026-11-03-001", "sku-1", 100))
      _          <- Console.printLine(s"Placed: $placed1 $placed2 $placed3, too many: $tooMany")
      november   <- DynamoDBQuery
                      .queryAll[Order]("orders")
                      .whereKey(
                        Order.customerId.partitionKey === "alice" && Order.orderId.sortKey.beginsWith("2026-11")
                      )
                      .execute
                      .flatMap(_.runCollect)
      _          <- Console.printLine(s"Alice's November orders: $november")
      firstPage  <- ordersPage("alice", None)
      secondPage <- ordersPage("alice", firstPage._2)
      _          <- Console.printLine(s"Pages: ${firstPage._1.size} + ${secondPage._1.size} orders")
      byKeyboard <- DynamoDBQuery
                      .queryAll[Order]("orders")
                      .indexName("orders-by-sku")
                      .whereKey(Order.sku.partitionKey === "sku-1")
                      .execute
                      .flatMap(_.runCollect)
      _          <- Console.printLine(s"Keyboard orders: $byKeyboard")
      cancelled  <- DynamoDBQuery
                      .deleteFrom("orders")(
                        Order.customerId.partitionKey === "alice" && Order.orderId.sortKey === "2026-11-01-001"
                      )
                      .returns(ReturnValues.AllOld)
                      .execute
      _          <- Console.printLine(s"Cancelled: $cancelled")
      _          <- dropTables
    } yield ()

  override def run = program.provide(localExecutor)
}
```

## Running the Program

With DynamoDB Local running (`docker compose -f docker-compose.yml up -d`), put `OrderStoreApp` in `src/main/scala` and start it:

```bash
sbt run
```

The program prints the product it found, the restocked product, which orders were placed (the order for 100 keyboards is refused), Alice's November orders, the sizes of two pages, the keyboard orders from the secondary index and the cancelled order. Stop DynamoDB Local afterwards with `docker compose -f docker-compose.yml down`.

## What You've Learned

In this tutorial, you learned:

- How a ZIO Schema and `ProjectionExpression.accessors` turn a case class into a DynamoDB item with typed attribute paths
- How a `DynamoDBQuery` describes a request, and how `DynamoDBQuery#execute` and the `DynamoDBExecutor` layer run it against DynamoDB Local or an in-memory executor
- How to create tables with composite keys and a global secondary index, and how to put, get, update and delete items
- How to query a partition with a key condition, page through results with `LastEvaluatedKey` and query a secondary index
- How to change two tables atomically with a transaction and guard it with condition expressions

You now have the whole life cycle of a small DynamoDB application in hand. The next step is to apply it to your own tables.

## Where to Go Next

- **Starting a new project?** [Getting Started](../getting-started.md) is the shortest path from an empty build to a first query.
- **Want every CRUD option?** The [CRUD Operations](../reference/high-level-api/crud-operations/index.md) reference covers each operation and its modifiers, and [Primary Keys](../reference/high-level-api/primary-keys.md) lists every key condition.
- **Need more on transactions?** [How to Run Operations in a Transaction](../how-to/run-operations-in-a-transaction.md) covers read transactions and the transaction rules.
- **Customising how your models are stored?** [How to Customize Sum Type Encoding](../how-to/customize-sum-type-encoding.md) shows the codec annotations.
