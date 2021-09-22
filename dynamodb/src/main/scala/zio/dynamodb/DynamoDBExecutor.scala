package zio.dynamodb

import zio.stm.TMap
import zio.{ Has, ULayer, ZIO }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] =
    (for {
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield Has.allOf[DynamoDBExecutor.Service, TestDynamoDBExecutor.Service](test, test)).toLayerMany
}
