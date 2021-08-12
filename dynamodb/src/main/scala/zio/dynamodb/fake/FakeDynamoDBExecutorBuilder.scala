package zio.dynamodb.fake

import zio.ULayer
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.{ Item, PrimaryKey }
import zio.stm.{ STM, TMap }

private[fake] final case class FakeDynamoDBExecutorBuilder private (
  private val tableInfos: List[TableSchemaAndData] = List.empty
) {
  self =>
  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): FakeDynamoDBExecutorBuilder =
    FakeDynamoDBExecutorBuilder(self.tableInfos :+ TableSchemaAndData(tableName, pkFieldName, entries.toList))

  def layer: ULayer[DynamoDBExecutor] =
    (for {
      tableMap       <- TMap.empty[String, TMap[PrimaryKey, Item]]
      tablePkNameMap <- TMap.empty[String, String]
      _              <- STM.foreach(self.tableInfos) { tableInfo =>
                          for {
                            _    <- tablePkNameMap.put(tableInfo.tableName, tableInfo.pkName)
                            tmap <- TMap.empty[PrimaryKey, Item]
                            _    <- STM.foreach(tableInfo.entries)(entry => tmap.put(entry._1, entry._2))
                            _    <- tableMap.put(tableInfo.tableName, tmap)
                          } yield ()
                        }
    } yield FakeDynamoDBExecutorImpl(tableMap, tablePkNameMap)).commit.toLayer

}
