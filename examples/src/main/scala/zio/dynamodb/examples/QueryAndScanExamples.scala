package zio.dynamodb.examples

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SortKeyExpression.SortKey
import zio.dynamodb._
import zio.stream.Stream
import zio.{ Chunk, ZIO }

object QueryAndScanExamples extends App {

  val tableName1: TableName = TableName("T1")
  val tableName2: TableName = TableName("T2")
  val indexName1            = IndexName("I1")

  val scanAll1: ZIO[DynamoDBExecutor, Exception, Stream[Exception, Item]]          =
    scanAll(tableName1, indexName1, $("A"), $("B"), $("C")).execute
  val scanSome2: ZIO[DynamoDBExecutor, Exception, (Chunk[Item], LastEvaluatedKey)] =
    scanSome(tableName1, indexName1, limit = 10, $("A"), $("B"), $("C")).execute

  val queryAll1: ZIO[DynamoDBExecutor, Exception, Stream[Exception, Item]] =
    queryAll(tableName1, indexName1, $("A"), $("B"), $("C"))
      .whereKey(
        PartitionKey("partitionKey1") === "x" &&
          SortKey("sortKey1") > "X"
      )
      .execute

  val querySome2: ZIO[DynamoDBExecutor, Exception, (Chunk[Item], LastEvaluatedKey)] =
    querySome(tableName1, indexName1, limit = 10, $("A"), $("B"), $("C"))
      .sortOrder(ascending = false)
      .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
      .execute

}
