package zio.dynamodb

import scala.util.control.NoStackTrace

sealed trait DynamoDBError extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBError {
  sealed trait DynamoDBItemError extends DynamoDBError

  final case class ValueNotFound(message: String) extends DynamoDBItemError
  final case class DecodingError(message: String) extends DynamoDBItemError
}
