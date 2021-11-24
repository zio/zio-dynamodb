package zio.dynamodb

sealed trait ConsistencyMode

object ConsistencyMode {
  case object Strong extends ConsistencyMode
  case object Weak   extends ConsistencyMode

  private[dynamodb] def toBoolean(consistencyMode: ConsistencyMode): Boolean =
    consistencyMode match {
      case Strong => true
      case Weak   => false
    }
}
