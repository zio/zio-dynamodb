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

/**
 * The update expression of an `UpdateItem` request: the actions to apply to an item of type `A`. The query builders
 * `DynamoDBQuery.update` and `DynamoDBQuery.updateItem` wrap the action they are given in one, so it is rarely built by
 * hand.
 *
 * Actions come from the operators of a [[ProjectionExpression]] (`set`, `setIfNotExists`, `append`, `prepend`,
 * `remove`, `add`, `addSet`, `deleteFromSet`) and are combined with `+`. The rendered expression groups them by kind,
 * in the order `SET`, `REMOVE`, `ADD`, `DELETE`; actions of the same kind keep the order they were added in.
 *
 * {{{
 * import zio.schema.{ DeriveSchema, Schema }
 *
 * final case class Product(sku: String, price: Int, stock: Int, labels: Set[String])
 * object Product {
 *   implicit val schema: Schema.CaseClass4[String, Int, Int, Set[String], Product] = DeriveSchema.gen[Product]
 *   val (sku, price, stock, labels) = ProjectionExpression.accessors[Product]
 * }
 *
 * val restock: UpdateExpression.Action[Product] =
 *   Product.price.set(1299) + Product.stock.add(50) + Product.labels.deleteFromSet(Set("sold-out"))
 *
 * val query = DynamoDBQuery.update("products")(Product.sku.partitionKey === "sku-42")(restock)
 * }}}
 */
final case class UpdateExpression[-A](action: Action[A]) extends Renderable { self =>

  /**
   * Renders the action as the text of a DynamoDB update expression, such as `set #n0 = :v1 add #n2 :v3`. Attribute
   * names are replaced by `#n` aliases and values by `:v` placeholders.
   */
  def render: AliasMapRender[String] =
    action.render
}

/** Holds the action and set operand types of an [[UpdateExpression]]. */
object UpdateExpression {

  /**
   * One update action on an item of type `From` (`SET`, `REMOVE`, `ADD` or `DELETE` on one path), or several actions
   * joined with `+`. The update operators of [[ProjectionExpression]] return them.
   */
  sealed trait Action[-From] { self =>

    /** Returns an action that applies both this action and `that`. */
    def +[A1 <: From](that: RenderableAction[A1]): Action[A1]

    /**
     * Renders this action as the text of an update expression. Several actions are grouped by kind, in the order `set`,
     * `remove`, `add`, `delete`, with the actions of each kind separated by commas.
     */
    def render: AliasMapRender[String] = {
      def generateActionsStatements[A <: RenderableAction[_]](chunk: Chunk[A]) =
        chunk.foldLeft(AliasMapRender.empty.map(_ => "")) { case (acc, action) =>
          acc.zipWith(action.miniRender) { case (acc, action) =>
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
          for {
            s    <- operand.render
            path <- AliasMapRender.getOrInsert(path)
          } yield s"set $path = $s"
        case Action.RemoveAction(path)        => AliasMapRender.getOrInsert(path).map(path => s"remove $path")
        case Action.AddAction(path, value)    =>
          for {
            path <- AliasMapRender.getOrInsert(path)
            v    <- AliasMapRender.getOrInsert(value)
          } yield s"add $path $v"
        case Action.DeleteAction(path, value) =>
          for {
            path <- AliasMapRender.getOrInsert(path)
            v    <- AliasMapRender.getOrInsert(value)
          } yield s"delete $path $v"
      }
    }

    /**
     * Splits `actions` by kind and returns the `SET`, `REMOVE`, `ADD` and `DELETE` actions, in that order, each in the
     * order they appear in `actions`.
     */
    def collectActions(actions: Chunk[RenderableAction[_]]) = {
      val sets    = ChunkBuilder.make[Action.SetAction[_, _]]()
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

  /**
   * A single update action on one path: `SET`, `REMOVE`, `ADD` or `DELETE`. Combine actions with `+`.
   */
  sealed trait RenderableAction[-A] extends Action[A] { self =>
    override def +[A1 <: A](that: RenderableAction[A1]): Action[A1] = Actions(Chunk(self, that))

    /**
     * Renders this action without its keyword, such as `#n0 = :v1` for a `SET` or `#n0 :v1` for an `ADD`, so that
     * several actions of one kind can share one keyword.
     */
    def miniRender: AliasMapRender[String] =
      self match {
        case Action.SetAction(path, operand)  =>
          for {
            path <- AliasMapRender.getOrInsert(path)
            s    <- operand.render
          } yield s"$path = $s"
        case Action.RemoveAction(path)        => AliasMapRender.getOrInsert(path)
        case Action.AddAction(path, value)    =>
          for {
            path <- AliasMapRender.getOrInsert(path)
            v    <- AliasMapRender.getOrInsert(value)
          } yield s"$path $v"
        case Action.DeleteAction(path, value) =>
          for {
            path <- AliasMapRender.getOrInsert(path)
            v    <- AliasMapRender.getOrInsert(value)
          } yield s"$path $v"
      }

  }

  /** The kinds of [[UpdateExpression.Action]]. */
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

  /**
   * The value on the right of a `SET` action: a value, a path, `list_append(...)`, `if_not_exists(...)`, or the sum or
   * difference of two operands.
   */
  sealed trait SetOperand[-To] { self =>
    import SetOperand._

    /** Returns the operand `this + that`. */
    def +[A <: To](that: SetOperand[A]): SetOperand[A] = Plus(self, that)

    /** Returns the operand `this - that`. */
    def -[A <: To](that: SetOperand[A]): SetOperand[A] = Minus(self, that)

    /** Renders this operand as the text of an update expression, such as `list_append(#n0, :v1)`. */
    def render: AliasMapRender[String] =
      self match {
        case Minus(left, right)                                                   =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l - $r" }
        case Plus(left, right)                                                    =>
          left.render
            .zipWith(right.render) { case (l, r) => s"$l + $r" }
        case ValueOperand(value)                                                  => AliasMapRender.getOrInsert(value).map(identity)
        case PathOperand(path)                                                    => AliasMapRender.getOrInsert(path)
        case ListAppend(projectionExpression, list)                               =>
          for {
            pe   <- AliasMapRender.getOrInsert(projectionExpression)
            list <- AliasMapRender.getOrInsert(list)
          } yield s"list_append($pe, $list)"
        case ListPrepend(projectionExpression, list)                              =>
          for {
            pe   <- AliasMapRender.getOrInsert(projectionExpression)
            list <- AliasMapRender.getOrInsert(list)
          } yield s"list_append($list, $pe)"
        case IfNotExists(projectionExpression: ProjectionExpression[_, _], value) =>
          for {
            pe <- AliasMapRender.getOrInsert(projectionExpression)
            v  <- AliasMapRender.getOrInsert(value)
          } yield s"if_not_exists($pe, $v)"
      }

  }

  /** The kinds of [[UpdateExpression.SetOperand]]. */
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
