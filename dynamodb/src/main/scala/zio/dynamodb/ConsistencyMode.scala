package zio.dynamodb

/**
 * Whether a read is strongly consistent. [[ConsistencyMode.Strong]] sends a consistent read flag of `true` and
 * [[ConsistencyMode.Weak]] sends `false` in get, query and scan requests. Currently, when `DynamoDBQuery#execute` sends
 * several gets as one batch get request, no consistent read flag is sent, so those reads are eventually consistent
 * whatever the mode.
 */
sealed trait ConsistencyMode

/** The consistency modes of a read. */
object ConsistencyMode {

  /** A strongly consistent read. */
  case object Strong extends ConsistencyMode

  /** An eventually consistent read. */
  case object Weak extends ConsistencyMode

  private[dynamodb] def toBoolean(consistencyMode: ConsistencyMode): Boolean =
    consistencyMode match {
      case Strong => true
      case Weak   => false
    }
}
