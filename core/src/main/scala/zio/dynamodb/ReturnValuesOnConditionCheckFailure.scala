package zio.dynamodb

sealed trait ReturnValuesOnConditionCheckFailure

object ReturnValuesOnConditionCheckFailure {
  case object None   extends ReturnValuesOnConditionCheckFailure
  case object AllOld extends ReturnValuesOnConditionCheckFailure
}
