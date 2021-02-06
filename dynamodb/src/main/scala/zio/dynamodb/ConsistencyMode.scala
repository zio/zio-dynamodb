package zio.dynamodb

sealed trait ConsistencyMode

object ConsistencyMode {
  case object Strong extends ConsistencyMode
  case object Weak   extends ConsistencyMode
}
