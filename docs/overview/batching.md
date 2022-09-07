---
id: overview_batching
title: "Batching and Parallelisation"
---

# Auto Batching
ZIO-DynamoDB will automatically batch requests for you if they are batchable and if they are zipped together. 

## Batch Get
The example below shows multiple get requests being zipped together. ZIO-DynamoDB will group these three get requests into a single BatchGetItem request instead of submitting multiple individual GetItem requests.
```scala
import zio.dynamodb._

val tableName      = TableName("T1")
val tableName2     = TableName("T2")
val primaryKeyT1   = PrimaryKey("k1" -> "v1")
val primaryKeyT1_2 = PrimaryKey("k1" -> "v2")
val primaryKeyT2   = PrimaryKey("k2" -> "v2")

val getItemT1   = GetItem(key = primaryKeyT1, tableName = tableName)
val getItemT1_2 = GetItem(key = primaryKeyT1_2, tableName = tableName)
val getItemT2   = GetItem(key = primaryKeyT2, tableName = tableName2)

val itemT1: Item   = getItemT1.key
val itemT1_2: Item = getItemT1_2.key
val itemT2: Item   = getItemT2.key

for {
  result  <- (getItemT1 zip getItemT1_2 zip getItemT2).execute
  expected = (Some(itemT1), Some(itemT1_2), Some(itemT2))
} yield assert(result)(equalTo(expected))
```
During execution the individual `GetItem`s are grouped together into a single `BatchGetItem`. The response is then broken up to match the structure of your original requests in the form of a tuple.

## Batch Write
Writes can also be batched together according to AWS's docs. Only `Put`s and `Delete`s can be batched into a single `BatchWriteItem` request. 

```scala
def putAndDelete(): ZIO[DynamoDBExecutor, Throwable, (Unit, Unit)] = {
  val putItem = PutItem(tableName = tableName3, item = primaryKeyT3_2)
  val deleteItem = DeleteItem(tableName = tableName1, key = primaryKeyT1)

  for {
    result <- (putItem zip deleteItem).execute
  } yield result
}
```


# Parallelisation

ZIO-DynamoDB also supports zipping queries that are not batchable according to AWS's docs. Instead of building a single batch request for these zipped queries we instead batch what we can and execute multiple requests in parallel. 

```scala



```




