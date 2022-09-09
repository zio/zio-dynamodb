---
id: overview_parallelisation
title: "Parallelisation"
---

## Parallelisation

ZIO-DynamoDB also supports zipping queries that are not batchable according to AWS's docs. Instead of building a single batch request for these zipped queries we instead batch what we can and execute multiple requests in parallel.

```scala
val putItem = put[Student]("tableName", student)
val getItem = get[Student]("tableName", otherStudent)

for {
  result <- (putItem zip getItem).execute
} yield result
```

In the example above the `putItem` and `getItem` requests are executed in parallel as there is no AWS API for batching a `PutItemRequest` and a `GetItemRequest` together.




