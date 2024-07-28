package zio.dynamodb

import zio.aws.dynamodb.DynamoDb
import zio.stm.{ STM, TMap }
import zio.{ Ref, ULayer, URLayer, ZIO, ZLayer }

trait DynamoDBExecutor {
  def execute[A](query: DynamoDBQuery[_, A]): ZIO[Any, DynamoDBError, A]
}

object DynamoDBExecutor {
  val live: URLayer[DynamoDb, DynamoDBExecutor] =
    ZLayer.fromZIO(for {
      db <- ZIO.service[DynamoDb]
    } yield DynamoDBExecutorImpl(db))

  lazy val test: ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      ref  <- Ref.make(List.empty[DynamoDBQuery[_, _]])
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield TestDynamoDBExecutorImpl(ref, tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

  def test(tableDefs: TableNameAndPK*): ULayer[DynamoDBExecutor with TestDynamoDBExecutor] = {
    val effect = for {
      ref  <- Ref.make(List.empty[DynamoDBQuery[_, _]])
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                  _              <- STM.foreach(tableDefs) {
                                      case (tableName, pkFieldName) =>
                                        for {
                                          _           <- tablePkNameMap.put(tableName, pkFieldName)
                                          pkToItemMap <- TMap.empty[PrimaryKey, Item]
                                          _           <- tableMap.put(tableName, pkToItemMap)
                                        } yield ()
                                    }
                } yield TestDynamoDBExecutorImpl(ref, tableMap, tablePkNameMap)).commit
    } yield test
    ZLayer.fromZIO(effect)
  }

}
