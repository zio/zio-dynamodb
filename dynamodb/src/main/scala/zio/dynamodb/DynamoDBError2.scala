package zio.dynamodb

import scala.util.control.NoStackTrace
import zio.Chunk

sealed trait DynamoDBError2 extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBError2 {
  final case class ValueNotFound(message: String) extends DynamoDBError2
  final case class DecodingError(message: String) extends DynamoDBError2
}

sealed trait DynamoDBBatchError2
    extends Throwable
    with DynamoDBError2
    with NoStackTrace
    with Product
    with Serializable

object DynamoDBBatchError2 {
  sealed trait Write                       extends Product with Serializable
  final case class Delete(key: PrimaryKey) extends Write
  final case class Put(item: Item)         extends Write

  final case class BatchWriteError(unprocessedItems: Map[String, Chunk[Write]]) extends DynamoDBBatchError2 {
    val message = "BatchWriteError: unprocessed items returned by aws"
  }

  final case class BatchGetError(unprocessedKeys: Map[String, Set[PrimaryKey]]) extends DynamoDBBatchError2 {
    val message = "BatchGetError: unprocessed keys returned by aws"
  }
}
