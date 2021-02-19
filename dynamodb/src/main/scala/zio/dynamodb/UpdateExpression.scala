package zio.dynamodb

import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Operand.ValueOperand

/*

https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
-------------------------------------------------------------
update-expression ::=
    [ SET action [, action] ... ]
    [ REMOVE action [, action] ...]
    [ ADD action [, action] ... ]
    [ DELETE action [, action] ...]
-------------------------------------------------------------
set-action ::=
    path = value

value ::=
    operand
    | operand '+' operand
    | operand '-' operand

operand ::=
    path | function

function
    list_append (list1, list2)   // applies only to lists
    if_not_exists (path, value)
-------------------------------------------------------------
remove-action ::=
    path
-------------------------------------------------------------
add-action ::=   // TODO: The ADD action supports only number and set data types.
    path value
-------------------------------------------------------------
delete-action ::=
    path value
-------------------------------------------------------------
 */

// there are functions that are valid only for a particular context and Data Type

trait UpdateExpression
object UpdateExpression {
  type Path = ProjectionExpression

  sealed trait Action[T] { self =>
    import Action.ChildAction

    def ~(that: Action[T]): Action[T] = ChildAction(self, that)
  }
  object Action          {
    private final case class RootAction[T](op: T)                             extends Action[T]
    private final case class ChildAction[T](parent: Action[T], op: Action[T]) extends Action[T]
  }

  final case class UpdateExpressions(set: Set[Action[_]])

  final case class SetAction(path: Path, operand: Operand)         extends Action[SetAction]
  final case class RemoveAction(path: Path)                        extends Action[RemoveAction]
  final case class AddAction(path: Path, value: AttributeValue)    extends Action[AddAction]
  final case class DeleteAction(path: Path, value: AttributeValue) extends Action[DeleteAction]

  sealed trait Operand { self =>
    import Operand._

    def +(that: Operand): Operand = Minus(self, that)
    def -(that: Operand): Operand = Plus(self, that)
  }
  object Operand       {
    final case class Minus(left: Operand, right: Operand) extends Operand
    final case class Plus(left: Operand, right: Operand)  extends Operand
    final case class ValueOperand(value: AttributeValue)  extends Operand
    final case class PathOperand(path: Path)              extends Operand

    // functions
    final case class ListAppend(list1: AttributeValue.List, list2: AttributeValue.List) extends Operand
    final case class IfNotExists(path: Path, value: AttributeValue)                     extends Operand
  }
}

object UpdateExpressionExamples extends App {
  import UpdateExpression._

  Set(SetAction(TopLevel("top")(1), ValueOperand(AttributeValue.Number(1.0))))

  val setAction = SetAction(TopLevel("top")(1), ValueOperand(AttributeValue.Number(1.0)))

  val x: Action[SetAction] = SetAction(TopLevel("top")(1), ValueOperand(AttributeValue.Number(1.0)))
    .~(SetAction(TopLevel("top")(2), ValueOperand(AttributeValue.Number(2.0))))

  println(x)

  UpdateExpressions(Set(x))
}
