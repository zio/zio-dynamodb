package zio.dynamodb

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

import scala.util.control.NoStackTrace

sealed trait DynamoDBError extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBError {
  sealed trait DynamoDBItemError extends DynamoDBError

  object DynamoDBItemError {
    final case class ValueNotFound(message: String) extends DynamoDBItemError
    final case class DecodingError(message: String) extends DynamoDBItemError
  }

  final case class DynamoDBAWSError(cause: DynamoDbException) extends DynamoDBError {
    override def message: String = cause.getMessage
  }
}
