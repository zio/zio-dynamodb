package zio.dynamodb

import zio.{ Chunk, ChunkBuilder }
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

    def +(that: RenderableAction): Action

    def render: AliasMapRender[String] =
//      def foldActionsIntoAliasMap[A <: RenderableAction](
//        chunk: Chunk[A],
//        zipper: AliasMapRender[List[String]]
//      ) =
//        chunk.foldLeft(zipper) {
//          case (acc, action) =>
//            acc.zipWith(action.miniRender) {
//              case (acc, action) => acc.appended(action)
//            }
//        }
      self match {
        // each action verb is only allowed to appear once, need to collect them
        case Actions(actions)                 =>
          val (sets, removes, adds, deletes) = collectActions(actions)
          val empty                          = AliasMapRender.empty.map(_ => "")
          val s                              = sets
            .foldLeft(empty) {
              case (acc, action) =>
                acc.zipWith(action.miniRender) {
                  case (acc, action) =>
                    acc ++ action
                }
            }
            .map(s => if (s.isEmpty) s else "set " ++ s)
          val r                              = removes
//            .foldLeft(empty) {
            .foldLeft(s) {
              case (acc, action) =>
                acc.zipWith(action.miniRender) {
                  case (acc, action) =>
                    acc ++ action
                }
            }
            .map(s => if (s.isEmpty) s else "remove " ++ s)
          val a                              = adds
//            .foldLeft(empty) {
            .foldLeft(r) {
              case (acc, action) =>
                acc.zipWith(action.miniRender) {
                  case (acc, action) =>
                    acc ++ action
                }
            }
            .map(s => if (s.isEmpty) s else "add " ++ s)
          val d                              = deletes
//            .foldLeft(empty) {
            .foldLeft(a) {
              case (acc, action) =>
                acc.zipWith(action.miniRender) {
                  case (acc, action) =>
                    acc ++ action
                }
            }
            .map(s => if (s.isEmpty) s else "delete " ++ s)

          d
//          a.zipWith(r.zipWith(s.zipWith(d) { case (add, delete) => add ++ " " ++ delete }) {
//            case (a, b) => a ++ " " ++ b
//          }) { case (a, b) => a ++ " " ++ b }
//            .map(_.trim)
        case Action.SetAction(path, operand)  =>
          operand.render.map(s => s"set $path = $s")
        case Action.RemoveAction(path)        => AliasMapRender.succeed(s"remove $path")
        case Action.AddAction(path, value)    =>
          AliasMapRender.getOrInsert(value).map(v => s"add $path $v")
        case Action.DeleteAction(path, value) =>
          AliasMapRender.getOrInsert(value).map(s => s"delete $path $s")
      }

    def collectActions(actions: Chunk[RenderableAction]) = {
      val sets    = ChunkBuilder.make[Action.SetAction]()
      val removes = ChunkBuilder.make[Action.RemoveAction]()
      val adds    = ChunkBuilder.make[Action.AddAction]()
      val deletes = ChunkBuilder.make[Action.DeleteAction]()

      actions.foreach {
        case set: Action.SetAction       => sets += set
        case remove: Action.RemoveAction => removes += remove
        case add: Action.AddAction       => adds += add
        case delete: Action.DeleteAction => deletes += delete
      }
      (sets.result(), removes.result(), adds.result(), deletes.result())
    }

  }

  sealed trait RenderableAction extends Action { self =>
    override def +(that: RenderableAction): Action = Actions(Chunk(self, that))
    def miniRender: AliasMapRender[String]         =
      self match {
        case Action.SetAction(path, operand)  =>
          operand.render.map(s => s"$path = $s")
        case Action.RemoveAction(path)        => AliasMapRender.succeed(path.toString)
        case Action.AddAction(path, value)    =>
          AliasMapRender.getOrInsert(value).map(v => s"$path $v")
        case Action.DeleteAction(path, value) =>
          AliasMapRender.getOrInsert(value).map(s => s"$path $s")
      }
  }
  object Action {

    private[dynamodb] final case class Actions(actions: Chunk[RenderableAction]) extends Action { self =>
      override def +(that: RenderableAction): Action = Actions(actions :+ that)

      def ++(that: Actions): Actions = Actions(actions ++ that.actions)
    }

    /**
     * Modifying or Adding item Attributes
     */
    private[dynamodb] final case class SetAction(path: ProjectionExpression, operand: SetOperand)
        extends RenderableAction

    /**
     * Removing Attributes from an item
     */
    private[dynamodb] final case class RemoveAction(path: ProjectionExpression) extends RenderableAction

    /**
     * Updating Numbers and Sets
     */
    private[dynamodb] final case class AddAction(path: ProjectionExpression, value: AttributeValue)
        extends RenderableAction

    /**
     * Delete Elements from a Set
     */
    private[dynamodb] final case class DeleteAction(path: ProjectionExpression, value: AttributeValue)
        extends RenderableAction
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
