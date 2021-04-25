package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.SortKeyExpression.SortKey
import zio.dynamodb.{ AttributeValue, KeyConditionExpression }

object QueryAndScanExamples extends App {

  val fieldA                               = TopLevel("A")
  val fieldB                               = TopLevel("B")
  val fieldC                               = TopLevel("C")
  val limit                                = 10
  val keyCondExprn: KeyConditionExpression =
    PartitionKey("partitionKey1") == AttributeValue.String("x") &&
      SortKey("sortKey1") > AttributeValue.String("X")

  val scan1 = scanAll(tableName1, indexName1, fieldA, fieldB, fieldC).execute
  val scan2 = scanSome(tableName1, indexName1, limit, fieldA, fieldB, fieldC).execute

  val query1 =
    queryAll(tableName1, indexName1, fieldA, fieldB, fieldC).whereKey(keyCondExprn).execute
  val query2 = querySome(tableName1, indexName1, limit, fieldA, fieldB, fieldC)
    .sortOrder(ascending = false)
    .whereKey(keyCondExprn)
    .execute

}
