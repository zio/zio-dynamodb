package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBExecutor.TestData._
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SortKeyExpression.SortKey
import zio.dynamodb.{ Item, KeyConditionExpression, LastEvaluatedKey }
import zio.{ stream, Chunk, ZIO }

object QueryAndScanExamples extends App {

  val fieldA                               = $("A")
  val fieldB                               = $("B")
  val fieldC                               = $("C")
  val limit                                = 10
  val keyCondExprn: KeyConditionExpression =
    PartitionKey("partitionKey1") === "x" &&
      SortKey("sortKey1") > "X"

  val x = $("foo.bar") > $("B")
  println(s"x=$x")

  val scanAll1: ZIO[DynamoDBExecutor, Exception, stream.Stream[Exception, Item]]   =
    scanAll(tableName1, indexName1, $("A"), $("B"), $("C")).execute
  val scanSome2: ZIO[DynamoDBExecutor, Exception, (Chunk[Item], LastEvaluatedKey)] =
    scanSome(tableName1, indexName1, limit, fieldA, fieldB, fieldC).execute

  val queryAll1: ZIO[DynamoDBExecutor, Exception, stream.Stream[Exception, Item]] =
    queryAll(tableName1, indexName1, fieldA, fieldB, fieldC).whereKey(keyCondExprn).execute

  val querySome2: ZIO[DynamoDBExecutor, Exception, (Chunk[Item], LastEvaluatedKey)] =
    querySome(tableName1, indexName1, limit, fieldA, fieldB, fieldC)
      .sortOrder(ascending = false)
      .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
      .execute

}
