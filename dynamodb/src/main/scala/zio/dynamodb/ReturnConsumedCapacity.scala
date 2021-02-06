package zio.dynamodb

sealed trait ReturnConsumedCapacity

object ReturnConsumedCapacity {
  case object Indexes extends ReturnConsumedCapacity
  case object Total   extends ReturnConsumedCapacity
  case object None    extends ReturnConsumedCapacity
}
