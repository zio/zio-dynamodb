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

  sealed trait Operand
  object Operand {
    type Path = ProjectionExpression

    final case class ValueOperand(value: AttributeValue) extends Operand
    final case class PathOperand(path: Path)             extends Operand
  }
}

sealed trait PartitionKeyExpression extends KeyConditionExpression { self =>
  import KeyConditionExpression.And

  def &&(that: SortKeyExpression): KeyConditionExpression = And(self, that)
}
object PartitionKeyExpression       extends KeyConditionExpression {
  import KeyConditionExpression._

  final case class PartitionKeyOperand(keyName: String) { self =>
    def ==(that: Operand): PartitionKeyExpression = Equals(self, that)
  }
  final case class Equals(left: PartitionKeyOperand, right: Operand) extends PartitionKeyExpression
}

sealed trait SortKeyExpression extends KeyConditionExpression

object SortKeyExpression {
  import KeyConditionExpression._

  final case class SortKeyOperand(keyName: String) { self =>
    def ==(that: Operand): SortKeyExpression = Equals(self, that)
    def <>(that: Operand): SortKeyExpression = NotEqual(self, that)
    def <(that: Operand): SortKeyExpression  = LessThan(self, that)
    def <=(that: Operand): SortKeyExpression = LessThanOrEqual(self, that)
    def >(that: Operand): SortKeyExpression  = GreaterThanOrEqual(self, that)
    def >=(that: Operand): SortKeyExpression = GreaterThanOrEqual(self, that)
  }

  final case class Equals(left: SortKeyOperand, right: Operand)             extends SortKeyExpression
  final case class NotEqual(left: SortKeyOperand, right: Operand)           extends SortKeyExpression
  final case class LessThan(left: SortKeyOperand, right: Operand)           extends SortKeyExpression
  final case class GreaterThan(left: SortKeyOperand, right: Operand)        extends SortKeyExpression
  final case class LessThanOrEqual(left: SortKeyOperand, right: Operand)    extends SortKeyExpression
  final case class GreaterThanOrEqual(left: SortKeyOperand, right: Operand) extends SortKeyExpression
}
