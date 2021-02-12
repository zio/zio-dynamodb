package zio.dynamodb

/*
~~GetItem~~
~~PutItem~~
~~Scan~~
UpdateItem
WriteItem ???
Query
ListTables
DescribeTable
 */

/*
POST UpdateItem request example
{
    "TableName": "Thread",
    "Key": {
        "ForumName": {
            "S": "Amazon DynamoDB"
        },
        "Subject": {
            "S": "Maximum number of items?"
        }
    },
    "UpdateExpression": "set LastPostedBy = :val1",
    "ConditionExpression": "LastPostedBy = :val2",
    "ExpressionAttributeValues": {
        ":val1": {"S": "alice@example.com"},
        ":val2": {"S": "fred@example.com"}
    },
    "ReturnValues": "ALL_NEW"
}

 */
