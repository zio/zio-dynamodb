package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.BatchGetItem
import zio.dynamodb.DynamoDBQuery.BatchWriteItem
import scala.util.control.NoStackTrace

/*
one option is to have everything batched
extra type paramerer on DynamoDBQuery to refect the difference between batched and non batched errors
 */

sealed trait DynamoDBError extends Exception with NoStackTrace {
  def message: String
  override def getMessage(): String = message
}

// it does not really make sense to unify ITEM and BATCH level errors

// TODO: this is really an ITEM level error
object DynamoDBError { // Item level errors

  sealed trait ItemError extends DynamoDBError

  // GetError
  // PutError
  final case class ValueNotFound(message: String) extends ItemError

  final case class DecodingError(message: String) extends ItemError

  //final case class WriteError(message: String, errorDetails: MapOfSet[String, String]) extends DynamoDBError

  // sealed trait Simple extends DynamoDBError eg ValueNotFound extends this

  final case class Both(left: DynamoDBError, right: DynamoDBError) /* extends DynamoDBError */ {
    def message = s"${left.message} and ${right.message}"
  }
}

trait DynamoDBBatchError extends Exception with NoStackTrace
object DynamoDBBatchError {
  // TODO: The Batch model is package private - so either we open it up
  // or create a parralel one just for batch error reporting

  final case class BatchWriteError(response: BatchWriteItem.Response) extends DynamoDBBatchError {
    def message = "Unpocessed items remaining after batch write request"
  }

  final case class BatchReadError(response: BatchGetItem.Response) extends DynamoDBBatchError {
    def message = "Unpocessed items remaining after batch read request"
  }

}
