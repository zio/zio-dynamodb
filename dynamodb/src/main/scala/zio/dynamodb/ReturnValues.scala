package zio.dynamodb

sealed trait ReturnValues

object ReturnValues {
  case object None       extends ReturnValues
  case object AllOld     extends ReturnValues
  case object UpdatedOld extends ReturnValues
  case object AllNew     extends ReturnValues
  case object UpdatedNew extends ReturnValues
}
