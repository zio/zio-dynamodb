package zio.dynamodb

import zio.schema.Schema

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

sealed trait ConditionExpression extends Renderable { self =>
  import ConditionExpression._

  def &&(that: ConditionExpression): ConditionExpression = And(self, that)
  def ||(that: ConditionExpression): ConditionExpression = Or(self, that)
  def unary_! : ConditionExpression                      = Not(self)

  def render: AliasMapRender[String] =
    self match {
      case Between(left, minValue, maxValue)  =>
        AliasMapRender.getOrInsert(minValue)
        for {
          l   <- left.render
          min <- AliasMapRender.getOrInsert(minValue)
          max <- AliasMapRender.getOrInsert(maxValue)
        } yield s"$l BETWEEN $min AND $max"
      case In(left, values)                   =>
        for {
          l    <- left.render
          vals <- values
                    .foldLeft(AliasMapRender.empty.map(_ => "")) {
                      case (acc, value) =>
                        acc.zipWith(AliasMapRender.getOrInsert(value)) {
                          case (acc, action) =>
                            if (acc.isEmpty) action
                            else s"$acc, $action"
                        }
                    }
        } yield s"$l IN ($vals)"
      case AttributeExists(path)              => AliasMapRender.succeed(s"attribute_exists($path)")
      case AttributeNotExists(path)           => AliasMapRender.succeed(s"attribute_not_exists($path)")
      case AttributeType(path, attributeType) => attributeType.render.map(v => s"attribute_type($path, $v)")
      case Contains(path, value)              => AliasMapRender.getOrInsert(value).map(v => s"contains($path, $v)")
      case BeginsWith(path, value)            => AliasMapRender.getOrInsert(value).map(v => s"begins_with($path, $v)")
      case And(left, right)                   => left.render.zipWith(right.render) { case (l, r) => s"($l) AND ($r)" }
      case Or(left, right)                    => left.render.zipWith(right.render) { case (l, r) => s"($l) OR ($r)" }
      case Not(exprn)                         => exprn.render.map(v => s"NOT ($v)")
      case Equals(left, right)                => left.render.zipWith(right.render) { case (l, r) => s"($l) = ($r)" }
      case NotEqual(left, right)              => left.render.zipWith(right.render) { case (l, r) => s"($l) <> ($r)" }
      case LessThan(left, right)              => left.render.zipWith(right.render) { case (l, r) => s"($l) < ($r)" }
      case GreaterThan(left, right)           => left.render.zipWith(right.render) { case (l, r) => s"($l) > ($r)" }
      case LessThanOrEqual(left, right)       => left.render.zipWith(right.render) { case (l, r) => s"($l) <= ($r)" }
      case GreaterThanOrEqual(left, right)    => left.render.zipWith(right.render) { case (l, r) => s"($l) >= ($r)" }
    }

}

// BNF  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
object ConditionExpression {
  private[dynamodb] sealed trait Operand { self =>
    def between(minValue: AttributeValue, maxValue: AttributeValue): ConditionExpression =
      Between(self, minValue, maxValue)
    def in(values: Set[AttributeValue]): ConditionExpression                             = In(self, values)

    // TODO: looks like these are never used
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
      GreaterThan(self, Operand.ValueOperand(t.toAttributeValue(that)))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): ConditionExpression =
      GreaterThanOrEqual(self, Operand.ValueOperand(t.toAttributeValue(that)))

    def render: AliasMapRender[String] =
      self match {
        case Operand.ProjectionExpressionOperand(pe) => AliasMapRender.succeed(pe.toString)
        case Operand.ValueOperand(value)             => AliasMapRender.getOrInsert(value).map(identity)
        case Operand.Size(path)                      => AliasMapRender.succeed(s"size($path)")
      }
  }

  /*
  its a problem cos it says we ca
   */
  object Operand {
    trait ToOperand[A] {
      def toOperand(a: A): Operand
    }
    object ToOperand extends ToOperandLowPriorityImplicits {

      implicit def fromSchemaAttributeValue[A](implicit schema: Schema[A]): ToOperand[A] = {
        val _ = schema
        new ToOperand[A] {
          override def toOperand(a: A): Operand = {
            val enc = Codec.encoder(schema)
            val av  = enc(a)
            println(s"fromSchemaAttributeValue av=${ValueOperand(av)}")
            ValueOperand(av) // TODO: review
          }
        }
      }

    }

    trait ToOperandLowPriorityImplicits {
      implicit def fromAttributeValue[A](implicit x: ToAttributeValue[A]): ToOperand[A] =
        new ToOperand[A] {
          override def toOperand(a: A): Operand = {
            println(s"fromAttributeValue ${ValueOperand(x.toAttributeValue(a))}")
            ValueOperand(x.toAttributeValue(a))
          }
        }

//      implicit def fromAttributeValueNothing[A](implicit x: ToAttributeValue[A]): ToOperand[Nothing, A] =
//        new ToOperand[Nothing, A] {
//          override def toOperand(a: A): Operand = {
//            println(s"fromAttributeValueNothing ${ValueOperand(x.toAttributeValue(a))}")
//            ValueOperand(x.toAttributeValue(a))
//          }
//        }

    }

    private[dynamodb] final case class ProjectionExpressionOperand(pe: ProjectionExpression[_]) extends Operand
    private[dynamodb] final case class ValueOperand(value: AttributeValue)                      extends Operand
    private[dynamodb] final case class Size(path: ProjectionExpression[_])                      extends Operand

  }

  private[dynamodb] final case class Between(left: Operand, minValue: AttributeValue, maxValue: AttributeValue)
      extends ConditionExpression
  private[dynamodb] final case class In(left: Operand, values: Set[AttributeValue]) extends ConditionExpression

  // functions
  private[dynamodb] final case class AttributeExists(path: ProjectionExpression[_])    extends ConditionExpression
  private[dynamodb] final case class AttributeNotExists(path: ProjectionExpression[_]) extends ConditionExpression
  private[dynamodb] final case class AttributeType(path: ProjectionExpression[_], attributeType: AttributeValueType)
      extends ConditionExpression
  private[dynamodb] final case class Contains(path: ProjectionExpression[_], value: AttributeValue)
      extends ConditionExpression
  private[dynamodb] final case class BeginsWith(path: ProjectionExpression[_], value: AttributeValue)
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
