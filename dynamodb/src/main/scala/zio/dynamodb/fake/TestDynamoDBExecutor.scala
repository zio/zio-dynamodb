package zio.dynamodb.fake

import zio.dynamodb.{ DynamoDBExecutor, Item, PrimaryKey }
import zio.stm.TMap
import zio.{ Has, UIO, ULayer, ZIO }

object TestDynamoDBExecutor {
  type TestDynamoDBExecutor = Has[Service]

  trait Service {
    def addTable(tableName: String, pkFieldName: String)(entries: TableEntry*): UIO[Unit]
  }

  val test: ULayer[Has[DynamoDBExecutor.Service] with Has[Service]] =
    (for {
      test <- (for {
                  tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
                  tablePkNameMap <- TMap.empty[String, String]
                } yield FakeDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit
    } yield Has.allOf[DynamoDBExecutor.Service, TestDynamoDBExecutor.Service](test, test)).toLayerMany

  def addTable(tableName: String, pkFieldName: String)(
    entries: (PrimaryKey, Item)*
  ): ZIO[TestDynamoDBExecutor, Nothing, Unit] =
    ZIO.accessM[TestDynamoDBExecutor](_.get.addTable(tableName, pkFieldName)(entries: _*))
}
