package zio.dynamodb

import zio.stm.TMap
import zio.{ Has, ULayer, URLayer, ZIO }
import io.github.vigoo.zioaws.dynamodb.DynamoDb

trait DynamoDBExecutor {
  def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Throwable, A]
}

object DynamoDBExecutor {
  val live: URLayer[DynamoDb, Has[DynamoDBExecutor]] =
    ZIO.service[DynamoDb.Service].map(dynamo => DynamoDBExecutorImpl(dynamo)).toLayer

  val test: ULayer[Has[DynamoDBExecutor] with Has[TestDynamoDBExecutor]] =
    (for {
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield Has.allOf[DynamoDBExecutor, TestDynamoDBExecutor](test, test)).toLayerMany
}
