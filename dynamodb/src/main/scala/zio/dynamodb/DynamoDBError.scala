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
  sealed trait ItemError extends DynamoDBError

  object ItemError {
    final case class ValueNotFound(message: String) extends ItemError
    final case class DecodingError(message: String) extends ItemError
  }

  final case class AWSError(cause: DynamoDbException) extends DynamoDBError {
    override def message: String = cause.getMessage

  }

  sealed trait BatchError extends DynamoDBError

  object BatchError {
    sealed trait Write                       extends Product with Serializable
    final case class Delete(key: PrimaryKey) extends Write
    final case class Put(item: Item)         extends Write

    final case class WriteError(unprocessedItems: Map[String, Chunk[Write]]) extends BatchError {
      val message = "unprocessed items returned by aws"
    }

    final case class GetError(unprocessedKeys: Map[String, Set[PrimaryKey]]) extends BatchError {
      val message = "unprocessed keys returned by aws"
    }
  }

  sealed trait TransactionError extends DynamoDBError

  // TODO: rename stuff here as well
  object TransactionError {
    case object EmptyTransaction extends TransactionError {
      val message = "transaction is empty"
    }

    case object MixedTransactionTypes extends TransactionError {
      val message = "transaction contains both get and write actions"
    }

    final case class InvalidTransactionActions(invalidActions: NonEmptyChunk[DynamoDBQuery[Any, Any]])
        extends TransactionError {
      val message = "transaction contains invalid actions"
    }
  }

}
