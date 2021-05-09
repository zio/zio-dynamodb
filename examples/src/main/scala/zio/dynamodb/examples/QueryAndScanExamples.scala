package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.KeyConditionExpression
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.{ $, Root }
import zio.dynamodb.SortKeyExpression.SortKey

object QueryAndScanExamples extends App {

  val fieldA                               = Root("A")
  val fieldB                               = Root("B")
  val fieldC                               = Root("C")
  val limit                                = 10
  val keyCondExprn: KeyConditionExpression =
    PartitionKey("partitionKey1") === "x" &&
      SortKey("sortKey1") > "X"

  val x = $("foo.bar") > $("B")
  println(s"x=$x")

  val scan1 = scanAll(tableName1, indexName1, fieldA, fieldB, fieldC).execute
  val scan2 = scanSome(tableName1, indexName1, limit, fieldA, fieldB, fieldC).execute
  val scan3 = scanAll(tableName1, indexName1, $("A"), $("B"), $("C")).execute

  // TODO: expand with new API facilities
  val query1 =
    queryAll(tableName1, indexName1, fieldA, fieldB, fieldC).whereKey(keyCondExprn).execute
  val query2 = querySome(tableName1, indexName1, limit, fieldA, fieldB, fieldC)
    .sortOrder(ascending = false)
    .whereKey(keyCondExprn)
    .execute

}
