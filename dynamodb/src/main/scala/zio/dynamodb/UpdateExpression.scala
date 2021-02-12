package zio.dynamodb

import zio.dynamodb.ProjectionExpression.TopLevel
import zio.dynamodb.UpdateExpression.Operand.ValueOperand

/*
TODO: Note each action keyword can appear only once - not sure how to encode this
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

  final case class Set(path: Path, operand: Operand) extends UpdateExpression

  trait Operand  { self =>
    import Operand._

    def +(that: Operand): Operand = Minus(self, that)
    def -(that: Operand): Operand = Plus(self, that)
  }
  object Operand {
    final case class Minus(left: Operand, right: Operand) extends Operand
    final case class Plus(left: Operand, right: Operand)  extends Operand
    final case class ValueOperand(value: AttributeValue)  extends Operand
    final case class PathOperand(path: Path)              extends Operand

    // functions
    final case class ListAppend(list1: AttributeValue.List, list2: AttributeValue.List) extends Operand
    final case class IfNotExists(path: Path, value: AttributeValue)                     extends Operand
  }
}

object UpdateExpressionExamples {
  import UpdateExpression._

  Set(TopLevel("top")(1), ValueOperand(AttributeValue.Number(1.0)))
}
