package zio.dynamodb.fake

import zio.ULayer
import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor

private[fake] final case class FakeDynamoDBExecutorBuilder private (
  private val tableInfos: List[TableSchemaAndData] = List.empty
) { self =>

  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): FakeDynamoDBExecutorBuilder =
    FakeDynamoDBExecutorBuilder(self.tableInfos :+ TableSchemaAndData(tableName, pkFieldName, entries.toList))

  def layer: ULayer[DynamoDBExecutor] = FakeDynamoDBExecutorImpl.make(self.tableInfos).toLayer

}
