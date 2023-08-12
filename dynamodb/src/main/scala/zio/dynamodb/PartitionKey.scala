package zio.dynamodb

import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals
import zio.dynamodb.proofs.RefersTo

private[dynamodb] final case class PartitionKey[-From, +To](keyName: String) { self =>
  def ===[To1 >: To, To2: ToAttributeValue, IsPrimaryKey](
    value: To2
  )(implicit ev: RefersTo[To1, To2]): PartitionKeyEquals[From] = {
    val _ = ev
    PartitionKeyEquals(self, implicitly[ToAttributeValue[To2]].toAttributeValue(value))
  }
}
