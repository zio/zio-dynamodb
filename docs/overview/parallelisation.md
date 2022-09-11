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

All DynamoDB Queries are eligible for parallel execution, however there are situations where the AWS API will error. For instance running a `TransactWriteItems` request on the same item an `UpdateItem` makes a change to will result in a runtime error.
