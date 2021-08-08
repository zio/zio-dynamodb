package zio.dynamodb.fake

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor
import zio.{ Ref, ULayer }

private[fake] final case class FakeDynamoDBExecutorBuilder private (private val db: Database = new Database()) { self =>
  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): FakeDynamoDBExecutorBuilder =
    FakeDynamoDBExecutorBuilder(self.db.table(tableName, pkFieldName)(entries: _*))

  def layer: ULayer[DynamoDBExecutor] =
    (for {
      ref <- Ref.make(self.db)
    } yield new FakeDynamoDBExecutorImpl(ref)).toLayer
}
