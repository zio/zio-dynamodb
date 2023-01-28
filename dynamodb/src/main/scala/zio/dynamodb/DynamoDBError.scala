package zio.dynamodb

import scala.util.control.NoStackTrace

sealed trait DynamoDBError extends Exception with NoStackTrace {
  def message: String
  override def getMessage(): String = message
}

object DynamoDBError {
  final case class ValueNotFound(message: String) extends DynamoDBError
  final case class DecodingError(message: String) extends DynamoDBError
}
