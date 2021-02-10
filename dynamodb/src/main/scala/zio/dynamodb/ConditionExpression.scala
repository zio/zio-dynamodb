package zio.dynamodb

/* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

condition-expression ::=
      operand comparator operand
    | operand BETWEEN operand AND operand
    | operand IN ( operand (',' operand (, ...) ))
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | ( condition )

comparator ::=
    =
    | <>
    | <
    | <=
    | >
    | >=

function ::=
    attribute_exists (path)
    | attribute_not_exists (path)
    | attribute_type (path, type)
    | begins_with (path, substr)
    | contains (path, operand)
    | size (path)
 */

// ConditionExpression is implicitly Boolean
sealed trait ConditionExpression { self =>
  import ConditionExpression._

  def &&(that: ConditionExpression): ConditionExpression = And(self, that)
  def ||(that: ConditionExpression): ConditionExpression = Or(self, that)
  def unary_! : ConditionExpression                      = Not(self)
}
// BNF  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
object ConditionExpression       {
  type Path = ProjectionExpression
  final case class AttributeExists(path: Path)                                extends ConditionExpression
  final case class AttributeNotExists(path: Path)                             extends ConditionExpression
  final case class AttributeType(path: Path, `type`: AttributeValue.type)     extends ConditionExpression
  final case class Contains[T](path: Path, value: T)                          extends ConditionExpression
  final case class BeginsWith[T](path: Path, value: T)                        extends ConditionExpression
  // logical operators
  final case class And(left: ConditionExpression, right: ConditionExpression) extends ConditionExpression
  final case class Or(left: ConditionExpression, right: ConditionExpression)  extends ConditionExpression
  final case class Not(exprn: ConditionExpression)                            extends ConditionExpression
}
sealed trait Operand             { self =>
  import Operand._

  // can T be constrained further to AttributeValue.type ?
  def `=`[T](that: Operand): ConditionExpression = Equals(self, that)
  def <>[T](that: Operand): ConditionExpression  = NotEqual(self, that)
  def <[T](that: Operand): ConditionExpression   = LessThan(self, that)
  def <=[T](that: Operand): ConditionExpression  = LessThanOrEqual(self, that)
  def >[T](that: Operand): ConditionExpression   = GreaterThanOrEqual(self, that)
  def >=[T](that: Operand): ConditionExpression  = GreaterThanOrEqual(self, that)

}
object Operand                   {
  import ConditionExpression.Path

  final case class ValueOperand[T](value: T) extends Operand
  final case class PathOperand(path: Path)   extends Operand
  final case class Size(path: Path)          extends Operand

  final case class Equals(left: Operand, right: Operand)               extends ConditionExpression
  final case class NotEqual(left: Operand, right: Operand)             extends ConditionExpression
  final case class LessThan(left: Operand, right: Operand)             extends ConditionExpression
  final case class GreaterThan(left: Operand, right: Operand)          extends ConditionExpression
  final case class LessThanOrEqual(left: Operand, right: Operand)      extends ConditionExpression
  final case class GreaterThanOrEqual(left: Operand, right: Operand)   extends ConditionExpression
  final case class Between[T](left: Operand, minValue: T, maxValue: T) extends ConditionExpression
  final case class In[T](left: Operand, values: T*)                    extends ConditionExpression

}
