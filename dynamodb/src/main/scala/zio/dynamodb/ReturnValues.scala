package zio.dynamodb

/**
 * The item attributes that a write operation returns. DynamoDB recognizes only `None` and `AllOld` for `PutItem` and
 * `DeleteItem`.
 */
sealed trait ReturnValues

/** The values of `ReturnValues`. */
object ReturnValues {

  /** Returns nothing. */
  case object None extends ReturnValues

  /** Returns the whole item as it was before the write. */
  case object AllOld extends ReturnValues

  /** Returns only the attributes that the write changed, as they were before it. */
  case object UpdatedOld extends ReturnValues

  /** Returns the whole item as it is after the write. */
  case object AllNew extends ReturnValues

  /** Returns only the attributes that the write changed, as they are after it. */
  case object UpdatedNew extends ReturnValues
}
