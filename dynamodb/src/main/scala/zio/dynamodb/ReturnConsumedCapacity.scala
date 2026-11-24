package zio.dynamodb

/**
 * How much detail about the consumed capacity a request returns. The values map to the DynamoDB values `INDEXES`,
 * `TOTAL` and `NONE`.
 */
sealed trait ReturnConsumedCapacity

/** The values of `ReturnConsumedCapacity`. */
object ReturnConsumedCapacity {

  /** Returns the consumed capacity of the table and of each index the request used (`INDEXES`). */
  case object Indexes extends ReturnConsumedCapacity

  /** Returns only the total consumed capacity of the request (`TOTAL`). */
  case object Total extends ReturnConsumedCapacity

  /** Returns no consumed capacity (`NONE`). */
  case object None extends ReturnConsumedCapacity
}
