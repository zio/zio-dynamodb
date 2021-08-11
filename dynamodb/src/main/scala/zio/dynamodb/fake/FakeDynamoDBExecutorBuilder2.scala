package zio.dynamodb.fake

import zio.ULayer
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.dynamodb.{ Item, PrimaryKey }
import zio.stm.{ STM, TMap }

private[fake] final case class TableInfo(tableName: String, pkName: String, entries: List[TableEntry])

private[fake] final case class FakeDynamoDBExecutorBuilder2 private (
  private val tableInfos: List[TableInfo] = List.empty
) {
  self =>
  def table2(tableName: String, pkFieldName: String)(entries: TableEntry*): FakeDynamoDBExecutorBuilder2 = {

    val list: List[(PrimaryKey, Item)] = entries.toList
    FakeDynamoDBExecutorBuilder2(self.tableInfos :+ TableInfo(tableName, pkFieldName, list))
  }

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
    } yield Database2(tableMap, tablePkNameMap)).commit.toLayer

}
