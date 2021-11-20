package zio.dynamodb

import zio.Chunk
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.Actions

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
final case class UpdateExpression(action: Action) { self =>
  def render: AliasMapRender[String] =
    action.render
}

object UpdateExpression {

  sealed trait Action { self =>
    def +(that: Action): Action = Actions(Chunk(self) :+ that)

    def render: AliasMapRender[String] =
      self match {
        case Actions(actions)                 =>
          actions.foldLeft(AliasMapRender.empty.map(_ => "")) {
            case (acc, action) =>
              acc.zipWith(action.render) {
                case (acc, action) =>
                  if (acc.isEmpty) action
                  else s"$acc $action"
              }
          }
        case Action.SetAction(path, operand)  =>
          operand.render.map(s => s"set $path = $s")
        case Action.RemoveAction(path)        => AliasMapRender.succeed(s"remove $path")
        case Action.AddAction(path, value)    =>
          AliasMapRender.getOrInsert(value).map(v => s"add $path $v")
        case Action.DeleteAction(path, value) =>
          AliasMapRender.getOrInsert(value).map(s => s"delete $path $s")
      }

  }
  object Action {

    private[dynamodb] final case class Actions(actions: Chunk[Action]) extends Action { self =>
      override def +(that: Action): Action = Actions(actions :+ that)
    }

    /**
     * Modifying or Adding item Attributes
     */
    private[dynamodb] final case class SetAction(path: ProjectionExpression, operand: SetOperand) extends Action

    /**
     * Removing Attributes from an item
     */
    private[dynamodb] final case class RemoveAction(path: ProjectionExpression) extends Action

    /**
     * Updating Numbers and Sets
     */
    private[dynamodb] final case class AddAction(path: ProjectionExpression, value: AttributeValue) extends Action

    /**
     * Delete Elements from a Set
     */
    private[dynamodb] final case class DeleteAction(path: ProjectionExpression, value: AttributeValue) extends Action
  }

  sealed trait SetOperand { self =>
    import SetOperand._

    def +(that: SetOperand): SetOperand = Plus(self, that)
    def -(that: SetOperand): SetOperand = Minus(self, that)

    def render: AliasMapRender[String] =
      self match {
        case Minus(left, right)                                             =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l - $r" }
        case Plus(left, right)                                              =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l + $r" }
        case ValueOperand(value)                                            => AliasMapRender.getOrInsert(value).map(identity)
        case PathOperand(path)                                              => AliasMapRender.succeed(path.toString)
        case ListAppend(projectionExpression, list)                         =>
          AliasMapRender.getOrInsert(list).map(v => s"list_append($projectionExpression, $v)")
        case ListPrepend(projectionExpression, list)                        =>
          AliasMapRender.getOrInsert(list).map(v => s"list_append($v, $projectionExpression)")
        case IfNotExists(projectionExpression: ProjectionExpression, value) =>
          AliasMapRender.getOrInsert(value).map(v => s"if_not_exists($projectionExpression, $v)")
      }

  }
  object SetOperand {
    private[dynamodb] final case class Minus(left: SetOperand, right: SetOperand) extends SetOperand
    private[dynamodb] final case class Plus(left: SetOperand, right: SetOperand)  extends SetOperand
    private[dynamodb] final case class ValueOperand(value: AttributeValue)        extends SetOperand
    private[dynamodb] final case class PathOperand(path: ProjectionExpression)    extends SetOperand

    // functions
    // list_append takes two arguments, currently just assuming that we'll be using this to append to an existing item
    private[dynamodb] final case class ListAppend(projectionExpression: ProjectionExpression, list: AttributeValue.List)
        extends SetOperand
    private[dynamodb] final case class ListPrepend(
      projectionExpression: ProjectionExpression,
      list: AttributeValue.List
    )                                                                                                 extends SetOperand
    private[dynamodb] final case class IfNotExists(path: ProjectionExpression, value: AttributeValue) extends SetOperand
  }
}
