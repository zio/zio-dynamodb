---
id: scan-all
title: "scanAll"
---

`DynamoDBQuery.scanAll` reads every item of a table, or of a secondary index, and returns them as a ZIO `Stream`:

```scala
object DynamoDBQuery {
  def scanAll[A: Schema](tableName: String): DynamoDBQuery[A, Stream[Throwable, A]]
}
```

The examples use the equipment model from [Scan and Query Operations](index.md):

```scala mdoc:silent
import zio._
import zio.dynamodb._
import zio.schema.{ DeriveSchema, Schema }

case class Equipment(id: String, year: Int, name: String, price: Double)
object Equipment {
  implicit val schema: Schema.CaseClass4[String, Int, String, Double, Equipment] = DeriveSchema.gen[Equipment]

  val (id, year, name, price) = ProjectionExpression.accessors[Equipment]
}
```

Executing the query returns the stream; the items are read as the stream is consumed:

```scala mdoc:compile-only
val printAll: ZIO[DynamoDBExecutor, Throwable, Unit] =
  for {
    stream <- DynamoDBQuery.scanAll[Equipment]("equipment").execute
    _      <- stream.tap(equipment => ZIO.debug(s"equipment: $equipment")).runDrain
  } yield ()
```

## Filtering

`DynamoDBQuery#filter` keeps only the items that match a condition. DynamoDB still reads, and charges for, every item; the filter only reduces what is sent back:

```scala mdoc:compile-only
val expensive: ZIO[DynamoDBExecutor, Throwable, Chunk[Equipment]] =
  DynamoDBQuery
    .scanAll[Equipment]("equipment")
    .filter(Equipment.price > 1.0 && Equipment.name.beginsWith("Widget"))
    .execute
    .flatMap(_.runCollect)
```

Key conditions (`whereKey`) apply only to queries; on a scan they are ignored, so use [queryAll](query-all.md) to read one partition.

## Parallel Scans

Scanning a large table can take a long time. `DynamoDBQuery#parallel(n)` runs a [parallel scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan): the table is split into `n` segments, each scanned by its own fiber, and the items are merged into one stream in no particular order:

```scala mdoc:compile-only
val all: ZIO[DynamoDBExecutor, Throwable, Long] =
  DynamoDBQuery.scanAll[Equipment]("equipment").parallel(8).execute.flatMap(_.runCount)
```

## Options

These options apply to `DynamoDBQuery.scanAll`:

```scala
<SCAN_ALL_QUERY>
  .filter(<ConditionExpression>)   // e.g. Equipment.price > 1.0
  .parallel(<N>)                   // parallel scan with N segments
  .indexName(<IndexName>)          // scan a secondary index
  .consistency(<ConsistencyMode>)
  .startKey(<LastEvaluatedKey>)    // start after this key
  .selectAllAttributes | .selectAllProjectedAttributes | .selectSpecificAttributes | .selectCount
  .capacity(<ReturnConsumedCapacity>)
```
