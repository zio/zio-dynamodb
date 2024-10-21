---
id: scan-all
title: "scanAll"
---

```scala
  def scanAll[A: Schema](
    tableName: String
  ): DynamoDBQuery[A, Stream[Throwable, A]] = ???
```

The `scanAll` operation is used to scan all items in a table, and uses a ZIO stream to return the results.  

Note that scanning all items in a table can be an expensive operation in terms of elapsed time (and AWS bills!) - to speed things up the AWS API offers a parallel scanning mode which can be invoked in the High Level API using the `parallel` combinator - the results are merged back into a the results stream in an undetermined order.

```scala
for {
  _          <- put(tableName, Equipment("1", 2020, "Widget1", 1.0)).execute
  _          <- put(tableName, Equipment("1", 2021, "Widget1", 2.0)).execute
  stream     <- queryAll[Equipment](tableName)
                  .whereKey(Equipment.id.partitionKey === "1")
                  .execute
  equipments <- stream.runCollect
} yield ()
```

## Combinators

```scala
<SCAN_ALL_QUERY>
  .whereKey(<KeyConditionExpr>) // eg Equipment.id.partitionKey === "1" && Equipment.year.sortKey > 2020
  .filter(<ConditionExpression>) // eg Equipment.price > 1.0 
  .parallel(<N>) // executes a native DDB parallel scan on the server and merges the results back to the stream
```