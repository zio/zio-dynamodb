package zio.dynamodb

import zio.aws.dynamodb.DynamoDb
import zio.stm.{ STM, TMap }
import zio.{ ULayer, URLayer, ZIO, ZLayer }

trait DynamoDBExecutor {
  def execute[A](atomicQuery: DynamoDBQuery[_, A]): ZIO[Any, Throwable, A]
}

object DynamoDBExecutor {
  val live: URLayer[DynamoDb, DynamoDBExecutor] =
    ZLayer.fromZIO(for {
      db <- ZIO.service[DynamoDb]
    } yield DynamoDBExecutorImpl(db))

  lazy val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      test <- (for {
                tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                tablePkNameMap <- TMap.empty[String, String]
              } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

  def test(tableDefs: TableNameAndPK*): ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      test <- (for {
                tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                tablePkNameMap <- TMap.empty[String, String]
                _              <- STM.foreach(tableDefs) { case (tableName, pkFieldName) =>
                                    for {
                                      _           <- tablePkNameMap.put(tableName, pkFieldName)
                                      pkToItemMap <- TMap.empty[PrimaryKey, Item]
                                      _           <- tableMap.put(tableName, pkToItemMap)
                                    } yield ()
                                  }
              } yield TestDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

}
