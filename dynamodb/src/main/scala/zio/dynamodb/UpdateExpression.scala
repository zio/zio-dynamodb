package zio.dynamodb

import zio.dynamodb.UpdateExpression.Action

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

// Note this implementation does not preserve the original order of actions ie after "Set field1 = 1, field1 = 2"
// if this turns out to be a problem we could change the internal implementation
final case class UpdateExpression private (actions: NonEmptySet[Action]) { self =>
  def +(action: Action): UpdateExpression = UpdateExpression(self.actions + action)
}

object UpdateExpression {
  def apply(action: Action): UpdateExpression = UpdateExpression(NonEmptySet(action))

  sealed trait Action
  object Action {

    /**
     * Modifying or Adding Item Attributes
     */
    final case class SetAction(path: ProjectionExpression, operand: SetOperand) extends Action

    /**
     * Removing Attributes from an Item
     */
    final case class RemoveAction(path: ProjectionExpression) extends Action

    /**
     * Updating Numbers and Sets
     */
    final case class AddAction(path: ProjectionExpression, value: AttributeValue) extends Action

    /**
     * Delete Elements from a Set
     */
    final case class DeleteAction(path: ProjectionExpression, value: AttributeValue) extends Action
  }

  sealed trait SetOperand { self =>
    import SetOperand._

    def +(that: SetOperand): SetOperand = Minus(self, that)
    def -(that: SetOperand): SetOperand = Plus(self, that)
  }
  object SetOperand       {
    final case class Minus(left: SetOperand, right: SetOperand) extends SetOperand
    final case class Plus(left: SetOperand, right: SetOperand)  extends SetOperand
    final case class ValueOperand(value: AttributeValue)        extends SetOperand
    final case class PathOperand(path: ProjectionExpression)    extends SetOperand

    // functions
    final case class ListAppend(list1: AttributeValue.List, list2: AttributeValue.List) extends SetOperand
    final case class IfNotExists(path: ProjectionExpression, value: AttributeValue)     extends SetOperand
  }
}
