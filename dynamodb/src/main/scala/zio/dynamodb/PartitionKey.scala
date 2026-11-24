package zio.dynamodb

import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class PartitionKey[-From, +To](keyName: String)

/**
 * Operators that build key condition expressions from the partition key of a table or index, which a `partitionKey`
 * call on a [[ProjectionExpression]] returns.
 */
object PartitionKey {

  /**
   * Operators for a partition key whose value type is not known, such as `\$("id").partitionKey`. The type of the value
   * is taken from the argument.
   */
  implicit class PartitionKeyUnknownToOps[-From](val pk: PartitionKey[From, Unknown]) {

    /** Builds the key condition `key = value`. Add a sort key condition with `&&`. */
    def ===[To: ToAttributeValue](
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, ToAttributeValue[To].toAttributeValue(value))
  }

  /**
   * Operators for a partition key whose value type `To` is known, such as one taken from the accessors of a case class.
   * The value must be of type `To`.
   */
  implicit class PartitionKeyOps[-From, To: ToAttributeValue](val pk: PartitionKey[From, To]) {

    /** Builds the key condition `key = value`. Add a sort key condition with `&&`. */
    def ===(
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, ToAttributeValue[To].toAttributeValue(value))
  }

}
