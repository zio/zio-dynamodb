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
  // TODO(adam): Implement
  def render(): AliasMapRender[String] =
    AliasMapRender { aliasMap =>
      action.render().render(aliasMap)
    }
}

object UpdateExpression {

  sealed trait Action { self =>
    def +(that: Action): Action = Actions(Chunk(self) :+ that)

    def render(): AliasMapRender[String] =
      AliasMapRender { aliasMap =>
        self match {
          case Actions(actions)                 =>
            actions.foldLeft((aliasMap, "")) {
              case ((am, acc), action) =>
                val (a, b) = action.render().render(am)
                (a, s"$acc, $b")
            }
          case Action.SetAction(path, operand)  =>
            operand.render().map(s => s"set $path = $s").render(aliasMap)
          case Action.RemoveAction(path)        => (aliasMap, s"remove $path")
          case Action.AddAction(path, value)    =>
            AliasMapRender.getOrInsert(value).map(v => s"add $path $v").render(aliasMap)
          case Action.DeleteAction(path, value) =>
            AliasMapRender.getOrInsert(value).map(s => s"delete $path $s").render(aliasMap)
        }
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

    def +(that: SetOperand): SetOperand = Minus(self, that)
    def -(that: SetOperand): SetOperand = Plus(self, that)

    def render(): AliasMapRender[String] =
      AliasMapRender { aliasMap =>
        self match {
          case Minus(left, right)    =>
            left
              .render()
              .flatMap { l =>
                right.render().map(r => s"$l - $r")
              }
              .render(aliasMap)
          case Plus(left, right)     =>
            left
              .render()
              .flatMap { l =>
                right.render().map(r => s"$l + $r")
              }
              .render(aliasMap)
          case ValueOperand(value)   => AliasMapRender.getOrInsert(value).map(identity).render(aliasMap)
          case PathOperand(path)     => (aliasMap, path.toString)
          case ListAppend(_)         => ??? //list.value.foldLeft(aliasMap) { case (acc, a) => AliasMapRender.getOrInsert(a) }
          case ListPrepend(_)        => ??? //(aliasMap, list.render())
          case IfNotExists(_, value) => AliasMapRender.getOrInsert(value).map(_ => ???).render(aliasMap)
        }
      }

  }
  object SetOperand {
    private[dynamodb] final case class Minus(left: SetOperand, right: SetOperand) extends SetOperand
    private[dynamodb] final case class Plus(left: SetOperand, right: SetOperand)  extends SetOperand
    private[dynamodb] final case class ValueOperand(value: AttributeValue)        extends SetOperand
    private[dynamodb] final case class PathOperand(path: ProjectionExpression)    extends SetOperand

    // functions
    private[dynamodb] final case class ListAppend(list: AttributeValue.List)                          extends SetOperand
    private[dynamodb] final case class ListPrepend(list: AttributeValue.List)                         extends SetOperand
    private[dynamodb] final case class IfNotExists(path: ProjectionExpression, value: AttributeValue) extends SetOperand
  }
}
