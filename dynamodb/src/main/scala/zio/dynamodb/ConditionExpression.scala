package zio.dynamodb

/* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html

In the following syntax summary, an operand can be the following:
1) A top-level attribute name, such as Id, Title, Description, or ProductCategory
2) A document path that references a nested attribute
ALso, my observation is that operand can be an AttributeValue


condition-expression ::=
      operand comparator operand
    | operand BETWEEN operand AND operand
    | operand IN ( operand (',' operand (, ...) ))
    | function
    | condition AND condition
    | condition OR condition
    | NOT condition
    | ( condition ) // TODO: do we need this one - (ie Parenthesis) ?

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
object ConditionExpression {
  private[dynamodb] sealed trait Operand { self =>
    def between(minValue: AttributeValue, maxValue: AttributeValue): ConditionExpression =
      Between(self, minValue, maxValue)
    def in(values: Set[AttributeValue]): ConditionExpression                             = In(self, values)

    def ==(that: Operand): ConditionExpression = Equals(self, that)
    def <>(that: Operand): ConditionExpression = NotEqual(self, that)
    def <(that: Operand): ConditionExpression  = LessThan(self, that)
    def <=(that: Operand): ConditionExpression = LessThanOrEqual(self, that)
    def >(that: Operand): ConditionExpression  = GreaterThanOrEqual(self, that)
    def >=(that: Operand): ConditionExpression = GreaterThanOrEqual(self, that)

    def ==[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
      Equals(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def <>[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
      NotEqual(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def <[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression  =
      LessThan(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def <=[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
      LessThanOrEqual(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def >[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression  =
      GreaterThanOrEqual(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
      GreaterThanOrEqual(self, Operand.ValueOperand(t.toAttributeValue(that)))
  }
  object Operand                         {

    private[dynamodb] final case class ProjectionExpressionOperand(pe: ProjectionExpression) extends Operand
    private[dynamodb] final case class ValueOperand(value: AttributeValue)                   extends Operand
    private[dynamodb] final case class Size(path: ProjectionExpression)                      extends Operand
  }

  private[dynamodb] final case class Between(left: Operand, minValue: AttributeValue, maxValue: AttributeValue)
      extends ConditionExpression
  private[dynamodb] final case class In(left: Operand, values: Set[AttributeValue]) extends ConditionExpression

  // functions
  private[dynamodb] final case class AttributeExists(path: ProjectionExpression)    extends ConditionExpression
  private[dynamodb] final case class AttributeNotExists(path: ProjectionExpression) extends ConditionExpression
  private[dynamodb] final case class AttributeType(path: ProjectionExpression, attributeType: AttributeValueType)
      extends ConditionExpression
  private[dynamodb] final case class Contains(path: ProjectionExpression, value: AttributeValue)
      extends ConditionExpression
  private[dynamodb] final case class BeginsWith(path: ProjectionExpression, value: AttributeValue)
      extends ConditionExpression

  // logical operators
  private[dynamodb] final case class And(left: ConditionExpression, right: ConditionExpression)
      extends ConditionExpression
  private[dynamodb] final case class Or(left: ConditionExpression, right: ConditionExpression)
      extends ConditionExpression
  private[dynamodb] final case class Not(exprn: ConditionExpression) extends ConditionExpression

  // comparators
  private[dynamodb] final case class Equals(left: Operand, right: Operand)             extends ConditionExpression
  private[dynamodb] final case class NotEqual(left: Operand, right: Operand)           extends ConditionExpression
  private[dynamodb] final case class LessThan(left: Operand, right: Operand)           extends ConditionExpression
  private[dynamodb] final case class GreaterThan(left: Operand, right: Operand)        extends ConditionExpression
  private[dynamodb] final case class LessThanOrEqual(left: Operand, right: Operand)    extends ConditionExpression
  private[dynamodb] final case class GreaterThanOrEqual(left: Operand, right: Operand) extends ConditionExpression
}
