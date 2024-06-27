package zio.dynamodb

import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals
import zio.dynamodb.ProjectionExpression.Unknown

private[dynamodb] final case class PartitionKey[-From, +To](keyName: String)
private[dynamodb] object PartitionKey {
  implicit class PartitionKeyUnknownToOps[-From](val pk: PartitionKey[From, Unknown])         {
    def ===[To: ToAttributeValue](
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, implicitly[ToAttributeValue[To]].toAttributeValue(value))
  }
  implicit class PartitionKeyOps[-From, To: ToAttributeValue](val pk: PartitionKey[From, To]) {
    def ===(
      value: To
    ): PartitionKeyEquals[From] =
      PartitionKeyEquals(pk, implicitly[ToAttributeValue[To]].toAttributeValue(value))
  }

}
