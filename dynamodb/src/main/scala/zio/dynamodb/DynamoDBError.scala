package zio.dynamodb

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

import scala.util.control.NoStackTrace
import zio.Chunk
import zio.NonEmptyChunk

sealed trait DynamoDBError extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBError {
  // TODO: rename to ItemError
  sealed trait DynamoDBItemError extends DynamoDBError

  // TODO: rename to ItemError
  object DynamoDBItemError {
    final case class ValueNotFound(message: String) extends DynamoDBItemError
    final case class DecodingError(message: String) extends DynamoDBItemError
  }

  // TODO: rename to AWSError
  final case class DynamoDBAWSError(cause: DynamoDbException) extends DynamoDBError {
    override def message: String = cause.getMessage

  }

  // TODO: rename to BatchError
  sealed trait DynamoDBBatchError extends DynamoDBError

  // TODO: rename to BatchError
  object DynamoDBBatchError {
    sealed trait Write                       extends Product with Serializable
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    // TODO: rename to WriteError
    final case class BatchWriteError(unprocessedItems: Map[String, Chunk[Write]]) extends DynamoDBBatchError {
      val message = "BatchWriteError: unprocessed items returned by aws" // TODO remove "BatchWriteError: " part
    }

    // TODO: rename to GetError
    final case class BatchGetError(unprocessedKeys: Map[String, Set[PrimaryKey]]) extends DynamoDBBatchError {
      val message = "BatchGetError: unprocessed keys returned by aws" // TODO: remove "BatchGetError: "
    }
  }

  sealed trait DynamoDBTransactionError extends DynamoDBError

  // TODO: rename stuff here as well
  object DynamoDBTransactionError {
    final case object EmptyTransaction extends DynamoDBTransactionError {
      val message = "transaction is empty"
    }

    final case object MixedTransactionTypes extends DynamoDBTransactionError {
      val message = "transaction contains both get and write actions"
    }

    final case class InvalidTransactionActions(invalidActions: NonEmptyChunk[DynamoDBQuery[Any, Any]])
        extends DynamoDBTransactionError {
      val message = "transaction contains invalid actions"
    }
  }

}
