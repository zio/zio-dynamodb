package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SortKeyExpression.SortKey
import zio.dynamodb._
import zio.stream.Stream
import zio.{ Chunk, Has, ZIO }

object QueryAndScanExamples extends App {

  val scanAll1: ZIO[Has[DynamoDBExecutor], Exception, Stream[Exception, Item]]          =
    scanAllItem("tableName1", "indexName1", $("A"), $("B"), $("C")).execute
  val scanSome2: ZIO[Has[DynamoDBExecutor], Exception, (Chunk[Item], LastEvaluatedKey)] =
    scanSomeItem("tableName1", "indexName1", limit = 10, $("A"), $("B"), $("C")).execute

  val queryAll1: ZIO[Has[DynamoDBExecutor], Exception, Stream[Exception, Item]] =
    queryAll("tableName1", "indexName1", $("A"), $("B"), $("C"))
      .whereKey(
        PartitionKey("partitionKey1") === "x" &&
          SortKey("sortKey1") > "X"
      )
      .execute

  val querySome2: ZIO[Has[DynamoDBExecutor], Exception, (Chunk[Item], LastEvaluatedKey)] =
    querySome("tableName1", "indexName1", limit = 10, $("A"), $("B"), $("C"))
      .sortOrder(ascending = false)
      .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
      .selectCount
      .execute

  val zippedAndSorted = (scanSomeItem("tableName1", "indexName1", limit = 10, $("A"), $("B"), $("C"))
    zip
      querySome(
        "tableName1",
        "indexName1",
        limit = 10,
        $("A"),
        $("B"),
        $("C")
      )
        .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
        .selectCount).sortOrder(ascending = true)

}
