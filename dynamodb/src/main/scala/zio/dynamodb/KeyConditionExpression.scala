package zio.dynamodb

/*
KeyCondition expression is a restricted version of ConditionExpression where by
- partition exprn is required and can only use "=" equals comparison
- optionally AND can be used to add a sort key expression

eg partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
comparisons operators are the same as for Condition

 */

sealed trait KeyConditionExpression
object KeyConditionExpression {
  final case class And(left: PartitionKeyExpression, right: SortKeyExpression) extends KeyConditionExpression
// KeyName wrapper class over KeyName with == method that accepts AV and PE

//  final case class KeyName(value: String) {
//    def ==(that: AttributeValue): PartitionKeyExpression =
//      PartitionKeyExpression.Equals(PartitionKeyOperand(value), that)
//  }

// TODO: delete as we only deal with AttributeValues on the RHS
//  sealed trait Operand
//  object Operand {
//
//    // adding value to AV and PE
//    final case class ValueOperand(value: AttributeValue)     extends Operand
//    final case class PathOperand(path: ProjectionExpression) extends Operand
//  }
}

sealed trait PartitionKeyExpression extends KeyConditionExpression { self =>
  import KeyConditionExpression.And

  def &&(that: SortKeyExpression): KeyConditionExpression = And(self, that)
}
object PartitionKeyExpression       extends KeyConditionExpression {

  final case class PartitionKey(keyName: String) { self =>
    // TODO: lift using ToAttributeValue
    def ==(that: AttributeValue): PartitionKeyExpression = Equals(self, that)
  }
  final case class Equals(left: PartitionKey, right: AttributeValue) extends PartitionKeyExpression
}

sealed trait SortKeyExpression extends KeyConditionExpression

object SortKeyExpression {

  final case class SortKey(keyName: String) { self =>
    // TODO: lift using ToAttributeValue
    def ==(that: AttributeValue): SortKeyExpression                          = Equals(self, that)
    def <>(that: AttributeValue): SortKeyExpression                          = NotEqual(self, that)
    def <(that: AttributeValue): SortKeyExpression                           = LessThan(self, that)
    def <=(that: AttributeValue): SortKeyExpression                          = LessThanOrEqual(self, that)
    def >(that: AttributeValue): SortKeyExpression                           = GreaterThanOrEqual(self, that)
    def >=(that: AttributeValue): SortKeyExpression                          = GreaterThanOrEqual(self, that)
    def between(min: AttributeValue, max: AttributeValue): SortKeyExpression = Between(self, min, max)
    def beginsWith(value: AttributeValue): SortKeyExpression                 = BeginsWith(self, value)
  }

  final case class Equals(left: SortKey, right: AttributeValue)                     extends SortKeyExpression
  final case class NotEqual(left: SortKey, right: AttributeValue)                   extends SortKeyExpression
  final case class LessThan(left: SortKey, right: AttributeValue)                   extends SortKeyExpression
  final case class GreaterThan(left: SortKey, right: AttributeValue)                extends SortKeyExpression
  final case class LessThanOrEqual(left: SortKey, right: AttributeValue)            extends SortKeyExpression
  final case class GreaterThanOrEqual(left: SortKey, right: AttributeValue)         extends SortKeyExpression
  final case class Between(left: SortKey, min: AttributeValue, max: AttributeValue) extends SortKeyExpression
  final case class BeginsWith(left: SortKey, value: AttributeValue)                 extends SortKeyExpression
}
