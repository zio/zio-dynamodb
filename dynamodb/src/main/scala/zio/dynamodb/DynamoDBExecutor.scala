package zio.dynamodb

import zio.stm.TMap
import zio.{ Has, ULayer, URLayer, ZIO, ZLayer }
import io.github.vigoo.zioaws.dynamodb.DynamoDb

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    // Doesn't the actual execution of this require something?
    // For the original test one it makes sense since we don't need to make API calls
    // But for the real executor don't we need either an http client or the zio-aws-dynamodb Service?
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  val live: URLayer[DynamoDb, DynamoDBExecutor] = ZLayer.succeed(DynamoDBExecutorImpl(???))

  val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] =
    (for {
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield Has.allOf[DynamoDBExecutor.Service, TestDynamoDBExecutor.Service](test, test)).toLayerMany
}
