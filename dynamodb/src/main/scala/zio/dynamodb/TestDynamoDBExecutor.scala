package zio.dynamodb

import zio.{ UIO, ZIO }

object TestDynamoDBExecutor {

  trait Service {
    def addTable(tableName: String, pkFieldName: String)(entries: TableEntry*): UIO[Unit]
  }

  def addTable(tableName: String, pkFieldName: String)(
    entries: TableEntry*
  ): ZIO[TestDynamoDBExecutor, Nothing, Unit] =
    ZIO.accessM[TestDynamoDBExecutor](_.get.addTable(tableName, pkFieldName)(entries: _*))
}
