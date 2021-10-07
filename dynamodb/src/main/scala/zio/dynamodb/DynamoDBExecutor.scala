package zio.dynamodb

import zio.stm.TMap
import zio.{ Has, ULayer, ZIO }

trait DynamoDBExecutor {
  def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
}

object DynamoDBExecutor {

  val test: ULayer[Has[DynamoDBExecutor] with Has[TestDynamoDBExecutor]] =
    (for {
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield Has.allOf[DynamoDBExecutor, TestDynamoDBExecutor](test, test)).toLayerMany
}
