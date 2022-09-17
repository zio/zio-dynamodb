package zio.dynamodb

import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.Actions
import zio.{ Chunk, ChunkBuilder }

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
add-action ::=   // Note: The ADD action supports only number and set data types.
    path value
-------------------------------------------------------------
delete-action ::=
    path value
-------------------------------------------------------------
 */

// Note this implementation does not preserve the original order of actions ie after "Set field1 = 1, field1 = 2"
// if this turns out to be a problem we could change the internal implementation
final case class UpdateExpression[-A](action: Action[A]) extends Renderable { self =>
  def render: AliasMapRender[String] =
    action.render
}

object UpdateExpression {

  sealed trait Action[-A] { self =>

    def +[A1 <: A](that: RenderableAction[A1]): Action[A1]

    def render: AliasMapRender[String] = {
      def generateActionsStatements[A <: RenderableAction[_]](chunk: Chunk[A]) =
        chunk.foldLeft(AliasMapRender.empty.map(_ => "")) {
          case (acc, action) =>
            acc.zipWith(action.miniRender) {
              case (acc, action) =>
                if (acc.isEmpty) action
                else acc ++ "," ++ action
            }
        }

      self match {
        case Actions(actions)                 =>
          val (sets, removes, adds, deletes) = collectActions(actions)

          for {
            set    <- generateActionsStatements(sets)
                        .map(s => if (s.isEmpty) s else "set " ++ s)
            remove <- generateActionsStatements(removes)
                        .map(s => if (s.isEmpty) s else "remove " ++ s)
            add    <- generateActionsStatements(adds)
                        .map(s => if (s.isEmpty) s else "add " ++ s)
            delete <- generateActionsStatements(deletes)
                        .map(s => if (s.isEmpty) s else "delete " ++ s)
          } yield List(set, remove, add, delete).filter(_.nonEmpty).mkString(" ")
        case Action.SetAction(path, operand)  =>
          operand.render.map(s => s"set $path = $s")
        case Action.RemoveAction(path)        => AliasMapRender.succeed(s"remove $path")
        case Action.AddAction(path, value)    =>
          AliasMapRender.getOrInsert(value).map(v => s"add $path $v")
        case Action.DeleteAction(path, value) =>
          AliasMapRender.getOrInsert(value).map(s => s"delete $path $s")
      }
    }

    def collectActions(actions: Chunk[RenderableAction[_]]) = {
      val sets    = ChunkBuilder.make[Action.SetAction[_, _]]() // add a bunch of underscores
      val removes = ChunkBuilder.make[Action.RemoveAction[_]]()
      val adds    = ChunkBuilder.make[Action.AddAction[_]]()
      val deletes = ChunkBuilder.make[Action.DeleteAction[_]]()

      actions.foreach {
        case set: Action.SetAction[_, _]    => sets += set
        case remove: Action.RemoveAction[_] => removes += remove
        case add: Action.AddAction[_]       => adds += add
        case delete: Action.DeleteAction[_] => deletes += delete
      }
      (sets.result(), removes.result(), adds.result(), deletes.result())
    }

  }

  sealed trait RenderableAction[-A] extends Action[A] { self =>
    override def +[A1 <: A](that: RenderableAction[A1]): Action[A1] = Actions(Chunk(self, that))
    def miniRender: AliasMapRender[String]                          =
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

    private[dynamodb] final case class Actions[A](actions: Chunk[RenderableAction[A]]) extends Action[A] { self =>
      override def +[A1 <: A](that: RenderableAction[A1]): Action[A1] = Actions(actions :+ that)

      def ++[A1 <: A](that: Actions[A1]): Actions[A1] = Actions(actions ++ that.actions)
    }

    /**
     * Modifying or Adding item Attributes
     */
    private[dynamodb] final case class SetAction[From, A](path: ProjectionExpression[From, A], operand: SetOperand[A])
        extends RenderableAction[From]

    /**
     * Removing Attributes from an item
     */
    private[dynamodb] final case class RemoveAction[A](path: ProjectionExpression[A, _]) extends RenderableAction[A]

    /**
     * Updating Numbers and Sets
     */
    private[dynamodb] final case class AddAction[A](path: ProjectionExpression[A, _], value: AttributeValue)
        extends RenderableAction[A]

    /**
     * Delete Elements from a Set
     */
    private[dynamodb] final case class DeleteAction[A](path: ProjectionExpression[A, _], value: AttributeValue)
        extends RenderableAction[A]
  }

  sealed trait SetOperand[-To] { self =>
    import SetOperand._

    def +[A <: To](that: SetOperand[A]): SetOperand[A] = Plus(self, that)
    def -[A <: To](that: SetOperand[A]): SetOperand[A] = Minus(self, that)

    def render: AliasMapRender[String] =
      self match {
        case Minus(left, right)                                                   =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l - $r" }
        case Plus(left, right)                                                    =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l + $r" }
        case ValueOperand(value)                                                  => AliasMapRender.getOrInsert(value).map(identity)
        case PathOperand(path)                                                    => AliasMapRender.succeed(path.toString)
        case ListAppend(projectionExpression, list)                               =>
          AliasMapRender.getOrInsert(list).map(v => s"list_append($projectionExpression, $v)")
        case ListPrepend(projectionExpression, list)                              =>
          AliasMapRender.getOrInsert(list).map(v => s"list_append($v, $projectionExpression)")
        case IfNotExists(projectionExpression: ProjectionExpression[_, _], value) =>
          AliasMapRender.getOrInsert(value).map(v => s"if_not_exists($projectionExpression, $v)")
      }

  }
  object SetOperand {

    private[dynamodb] final case class Minus[A](left: SetOperand[A], right: SetOperand[A]) extends SetOperand[A]
    private[dynamodb] final case class Plus[A](left: SetOperand[A], right: SetOperand[A])  extends SetOperand[A]
    private[dynamodb] final case class ValueOperand[A](value: AttributeValue)              extends SetOperand[A]
    private[dynamodb] final case class PathOperand[A](path: ProjectionExpression[_, A])    extends SetOperand[A]

    // functions
    // list_append takes two arguments, currently just assuming that we'll be using this to append to an existing item
    private[dynamodb] final case class ListAppend[A](
      projectionExpression: ProjectionExpression[_, A],
      list: AttributeValue.List
    ) extends SetOperand[A]
    private[dynamodb] final case class ListPrepend[A](
      projectionExpression: ProjectionExpression[_, A],
      list: AttributeValue.List
    ) extends SetOperand[A]
    private[dynamodb] final case class IfNotExists[A](path: ProjectionExpression[_, A], value: AttributeValue)
        extends SetOperand[A]
  }
}
