---
id: query-combinators
title: "Query Combinators"
---

Query Combinators | Description
---|---
map |
zip <*>|
zipLeft (<*) |
zipRight (*>) |
zipWith |

Query Operations | Description
---|---
capacity | sets the ReturnConsumedCapacity. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-ReturnConsumedCapacity). Note capacity data in the response is ignored by the High Level Api
consistency | sets the `ConsistencyMode` for read operations. Valid values are `Strong`and `Weak`(default) [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-ConsistentRead)  
filter | sets the `FilterExpression` - applies to `ScanSome`, `ScanAll`, `QuerySome`, `QueryAll`. Note the filter is applies **after** the read by DDB so no read units are saved, however latency costs are reduced.
gsi | creates a Global Secondary Index - applies to a `CreateTable` query. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#DDB-CreateTable-request-GlobalSecondaryIndexes)
indexName | sets the local secondary index or global secondary index name - applies to `ScanSome`, `ScanAll`, `QuerySome`, `QueryAll`. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#DDB-Scan-request-IndexName) 
lsi | creates a local Secondary Index - applies to a `CreateTable` query. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#DDB-CreateTable-request-LocalSecondaryIndexes) 
metrics | set `ReturnItemCollectionMetrics`, valid values are `None` (default) and `Size` - applies to PutItem, UpdateItem, Delete, Transaction. Note that metric data in the response is ignored by the High Level API. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-ReturnItemCollectionMetrics)
parallel(N) | Applies only to `Scan` - sements and runs the query in parallel in DDB and merges the items in the response. N is level of parallelism. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan). 
returns | sets the `ReturnValues` - applies to `UpdateItem`, `DeleteItem`, `PutItem` (see [Crud Operations](reference/hi-level-api/crud-operations/index.md) reference section for each operation for more details). [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-ReturnValues)
selectAllAttributes, selectAllProjectedAttributes, selectSpecificAttributes, selectCount | Determines the attributes returned by Scan and Query [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-Select)    
sortOrder | sets the sort order for `Query`'s [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ScanIndexForward) 
startKey |Applies to `Query` and `Scan` and specifies the start key for the query. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-ExclusiveStartKey)
transaction | executes the query in a transaction - see [Transactions Guide](../../guides/transactions) for more details.
where | sets the `ConditionExpression` - applies to `PutItem`, `DeleteOtem`, `UpdateItem` and `Scan` [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html)
whereKey | set the `KeyConditionExpr` applies to `QuerySome` and `QueryAll`. [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression) 
withClientRequestToken | set the client request token` - applies to write transactions [AWS API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken)
withRetryPolicy | set the retry policy for a batched query - see TODO
