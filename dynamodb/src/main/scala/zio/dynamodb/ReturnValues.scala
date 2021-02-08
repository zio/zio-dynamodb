package zio.dynamodb

sealed trait ReturnValues
case object None       extends ReturnValues
case object AllOld     extends ReturnValues
case object UpdatedOld extends ReturnValues
case object UpdatedNew extends ReturnValues
