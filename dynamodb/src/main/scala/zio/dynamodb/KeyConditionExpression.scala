package zio.dynamodb

/*
KeyCondition expression is a restricted version of ConditionExpression where by
- partition exprn is required and can only use "=" equals comparison
- optionally AND can be used to add a sort key expression

eg partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval
comparisons operators are the same as for Condition

 */

sealed trait KeyConditionExpression {
  def render(): String = ???
}

object KeyConditionExpression {
  private[dynamodb] final case class And(left: PartitionKeyExpression, right: SortKeyExpression)
      extends KeyConditionExpression
}

sealed trait PartitionKeyExpression extends KeyConditionExpression { self =>
  import KeyConditionExpression.And

  def &&(that: SortKeyExpression): KeyConditionExpression = And(self, that)
}
object PartitionKeyExpression {
  final case class PartitionKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): PartitionKeyExpression =
      Equals(self, t.toAttributeValue(that))
  }
  final case class Equals(left: PartitionKey, right: AttributeValue) extends PartitionKeyExpression
}

sealed trait SortKeyExpression

object SortKeyExpression {

  final case class SortKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression            = Equals(self, t.toAttributeValue(that))
    def <>[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             = NotEqual(self, t.toAttributeValue(that))
    def <[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              = LessThan(self, t.toAttributeValue(that))
    def <=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      LessThanOrEqual(self, t.toAttributeValue(that))
    def >[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def between[A](min: A, max: A)(implicit t: ToAttributeValue[A]): SortKeyExpression =
      Between(self, t.toAttributeValue(min), t.toAttributeValue(max))
    def beginsWith[A](value: A)(implicit t: ToAttributeValue[A]): SortKeyExpression    =
      BeginsWith(self, t.toAttributeValue(value))
  }

  private[dynamodb] final case class Equals(left: SortKey, right: AttributeValue)             extends SortKeyExpression
  private[dynamodb] final case class NotEqual(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class LessThan(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class GreaterThan(left: SortKey, right: AttributeValue)        extends SortKeyExpression
  private[dynamodb] final case class LessThanOrEqual(left: SortKey, right: AttributeValue)    extends SortKeyExpression
  private[dynamodb] final case class GreaterThanOrEqual(left: SortKey, right: AttributeValue) extends SortKeyExpression
  private[dynamodb] final case class Between(left: SortKey, min: AttributeValue, max: AttributeValue)
      extends SortKeyExpression
  private[dynamodb] final case class BeginsWith(left: SortKey, value: AttributeValue)         extends SortKeyExpression
}
