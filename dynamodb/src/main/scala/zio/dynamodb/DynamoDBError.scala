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

  /**
   * Encapsulates the underlying AWS SDK dynamodb error in `cause` which can be pattern matched eg
   * `case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => ...`
   */
  final case class AWSError(cause: DynamoDbException) extends DynamoDBError {
    override def message: String = cause.getMessage

  }

  /**
   * You need to handle this error if queries result in batching ie if you are using `DynamoDBQuery.forEach` or utility
   * functions that use `DynamoDBQuery.forEach`. Note at the point that this error is raised automatic retries have already occurred.
   * For a long running process typical handler actions would be to record the errors and to carry on processing.
   */
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

  /**
   * You need to handle this error if you are using the transaction API ie `dynamoDBQuery.transaction` or `dynamoDBQuery.safeTransaction`
   */
  sealed trait TransactionError extends DynamoDBError

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
