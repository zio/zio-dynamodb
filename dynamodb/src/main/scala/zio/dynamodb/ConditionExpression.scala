package zio.dynamodb

import zio.dynamodb.ProjectionExpression.TopLevel

/* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

condition-expression ::=
      operand comparator operand
    | operand BETWEEN operand AND operand
    | operand IN ( operand (',' operand (, ...) ))
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | ( condition ) // TODO: forgot this one

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
  // unary_! shamelessly copied from FD course example
  def unary_! : ConditionExpression                      = Not(self)
}

// BNF  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
object ConditionExpression {
  type Path = ProjectionExpression

  final case class Between(left: Operand, minValue: AttributeValue, maxValue: AttributeValue)
      extends ConditionExpression
  final case class In(left: Operand, values: Set[AttributeValue]) extends ConditionExpression

  // functions
  final case class AttributeExists(path: Path)                           extends ConditionExpression
  final case class AttributeNotExists(path: Path)                        extends ConditionExpression
  final case class AttributeType(path: Path, `type`: AttributeValueType) extends ConditionExpression
  final case class Contains(path: Path, value: AttributeValue)           extends ConditionExpression
  final case class BeginsWith(path: Path, value: AttributeValue)         extends ConditionExpression

  // logical operators
  final case class And(left: ConditionExpression, right: ConditionExpression) extends ConditionExpression
  final case class Or(left: ConditionExpression, right: ConditionExpression)  extends ConditionExpression
  final case class Not(exprn: ConditionExpression)                            extends ConditionExpression

  // comparators
  final case class Equals(left: Operand, right: Operand)             extends ConditionExpression
  final case class NotEqual(left: Operand, right: Operand)           extends ConditionExpression
  final case class LessThan(left: Operand, right: Operand)           extends ConditionExpression
  final case class GreaterThan(left: Operand, right: Operand)        extends ConditionExpression
  final case class LessThanOrEqual(left: Operand, right: Operand)    extends ConditionExpression
  final case class GreaterThanOrEqual(left: Operand, right: Operand) extends ConditionExpression

  // Intention here is to track type so that later we can enforce 2 operands to be of same type
  sealed trait Operand { self =>

    def ==(that: Operand): ConditionExpression = Equals(self, that)
    def <>(that: Operand): ConditionExpression = NotEqual(self, that)
    def <(that: Operand): ConditionExpression  = LessThan(self, that)
    def <=(that: Operand): ConditionExpression = LessThanOrEqual(self, that)
    def >(that: Operand): ConditionExpression  = GreaterThanOrEqual(self, that)
    def >=(that: Operand): ConditionExpression = GreaterThanOrEqual(self, that)
  }
  object Operand {

    final case class ValueOperand(value: AttributeValue) extends Operand
    final case class PathOperand(path: Path)             extends Operand
    final case class Size(path: Path)                    extends Operand
  }
}

// TODO: remove
object ConditionExpressionExamples {

  import ConditionExpression.Operand._
  import ConditionExpression._

  val x: ConditionExpression = ValueOperand(AttributeValue.String("")) == ValueOperand(AttributeValue.String(""))
  val y: ConditionExpression = x && x

  val p: ConditionExpression =
    PathOperand(TopLevel("foo")(1)) > ValueOperand(AttributeValue.Number(1.0))

// does not compile - forces LHS and RHS operand types to match
//  val p2: ConditionExpression =
//    PathOperand[AttributeValue.Number](TopLevel("foo")(1)) > ValueOperand(AttributeValue.String("X"))

  val c    = AttributeType(TopLevel("foo")(1), AttributeValueType.Number) && p
  val notC = !c
}
