package zio.dynamodb.examples

import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.PartitionKeyExpression.PartitionKey
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SortKeyExpression.SortKey
import zio.dynamodb._
import zio.stream.Stream
import zio.{ Chunk, Has, ZIO }

object QueryAndScanExamples extends App {
  val scanAll: ZIO[Has[DynamoDBExecutor], Throwable, Stream[Throwable, Item]]                   =
    scanAllItem("tableName1", $("A"), $("B"), $("C")).execute
  val scanAllWithSecondaryIndex: ZIO[Has[DynamoDBExecutor], Throwable, Stream[Throwable, Item]] =
    scanAllItem("tableName1", $("A"), $("B"), $("C")).indexName("secondaryIndex").execute
  val scanSome: ZIO[Has[DynamoDBExecutor], Throwable, (Chunk[Item], LastEvaluatedKey)]          =
    scanSomeItem("tableName1", limit = 10, $("A"), $("B"), $("C")).execute

  val queryAll: ZIO[Has[DynamoDBExecutor], Throwable, Stream[Throwable, Item]] =
    queryAllItem("tableName1", $("A"), $("B"), $("C"))
      .whereKey(
        PartitionKey("partitionKey1") === "x" &&
          SortKey("sortKey1") > "X"
      )
      .execute

  val querySome: ZIO[Has[DynamoDBExecutor], Throwable, (Chunk[Item], LastEvaluatedKey)] =
    querySomeItem("tableName1", limit = 10, $("A"), $("B"), $("C"))
      .sortOrder(ascending = false)
      .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
      .selectCount
      .execute

  val zippedAndSorted = (scanSomeItem("tableName1", limit = 10, $("A"), $("B"), $("C"))
    zip
      querySomeItem(
        "tableName1",
        limit = 10,
        $("A"),
        $("B"),
        $("C")
      )
        .whereKey(PartitionKey("partitionKey1") === "x" && SortKey("sortKey1") > "X")
        .selectCount).sortOrder(ascending = true)

}
