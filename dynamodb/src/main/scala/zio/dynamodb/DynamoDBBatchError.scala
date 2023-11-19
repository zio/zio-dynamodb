package zio.dynamodb

import scala.util.control.NoStackTrace
import zio.Chunk

sealed trait DynamoDBBatchError extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBBatchError {
  sealed trait Write                       extends Product with Serializable
  final case class Delete(key: PrimaryKey) extends Write
  final case class Put(item: Item)         extends Write

  final case class BatchWriteError(message: String, unprocessedItems: Map[String, Chunk[Write]])
      extends DynamoDBBatchError
  final case class BatchGetError(message: String, unprocessedKeys: Map[String, Set[PrimaryKey]])
      extends DynamoDBBatchError
}
