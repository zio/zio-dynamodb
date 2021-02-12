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

  final case class Between[V <: AttributeValue](left: Operand[V], minValue: V, maxValue: V) extends ConditionExpression
  final case class In[V <: AttributeValue](left: Operand[V], values: Set[V])                extends ConditionExpression

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
  final case class Equals[V <: AttributeValue](left: Operand[V], right: Operand[V])          extends ConditionExpression
  final case class NotEqual[V <: AttributeValue](left: Operand[V], right: Operand[V])        extends ConditionExpression
  final case class LessThan[V <: AttributeValue](left: Operand[V], right: Operand[V])        extends ConditionExpression
  final case class GreaterThan[V <: AttributeValue](left: Operand[V], right: Operand[V])     extends ConditionExpression
  final case class LessThanOrEqual[V <: AttributeValue](left: Operand[V], right: Operand[V]) extends ConditionExpression
  final case class GreaterThanOrEqual[V <: AttributeValue](left: Operand[V], right: Operand[V])
      extends ConditionExpression

  // Intention here is to track type so that later we can enforce 2 operands to be of same type
  sealed trait Operand[V <: AttributeValue] { self =>

    def ==(that: Operand[V]): ConditionExpression = Equals(self, that)
    def <>(that: Operand[V]): ConditionExpression = NotEqual(self, that)
    def <(that: Operand[V]): ConditionExpression  = LessThan(self, that)
    def <=(that: Operand[V]): ConditionExpression = LessThanOrEqual(self, that)
    def >(that: Operand[V]): ConditionExpression  = GreaterThanOrEqual(self, that)
    def >=(that: Operand[V]): ConditionExpression = GreaterThanOrEqual(self, that)
  }
  object Operand {

    final case class ValueOperand[V <: AttributeValue](value: V)  extends Operand[V]
    final case class PathOperand[V <: AttributeValue](path: Path) extends Operand[V]
    final case class Size[V <: AttributeValue](path: Path)        extends Operand[V]
  }
}

// TODO: remove
object ConditionExpressionExamples {

  import ConditionExpression.Operand._
  import ConditionExpression._

  val x: ConditionExpression = ValueOperand(AttributeValue.String("")) == ValueOperand(AttributeValue.String(""))
  val y: ConditionExpression = x && x

  val p: ConditionExpression =
    PathOperand[AttributeValue.Number](TopLevel("foo")(1)) > ValueOperand(AttributeValue.Number(1.0))

// does not compile - forces LHS and RHS operand types to match
//  val p2: ConditionExpression =
//    PathOperand[AttributeValue.Number](TopLevel("foo")(1)) > ValueOperand(AttributeValue.String("X"))

  val c    = AttributeType(TopLevel("foo")(1), AttributeValueType.Number) && p
  val notC = !c
}
