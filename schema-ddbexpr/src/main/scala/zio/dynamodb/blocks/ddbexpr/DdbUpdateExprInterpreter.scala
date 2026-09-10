/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb.blocks.ddbexpr

import zio.blocks.schema.{ Optic, Reflect, Schema }
import zio.blocks.schema.binding.Binding
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfig
import zio.dynamodb.blocks.ProjectionResolver
import zio.dynamodb.{ AttributeValue, ProjectionExpression, UpdateExpression }
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand

/**
 * Interprets a [[DdbUpdateExpr]][From] into `core`'s [[UpdateExpression.Action]][From] — the
 *  update-action counterpart of [[DdbExprInterpreter]].
 *
 *  Attribute paths are resolved and operand literals encoded through an [[ExprCtx]]:
 *  per-[[Table]] on the configured path (so `update` writes the same attribute names and
 *  literal encodings as `put` does for the item body), or the shared [[ExprCtx.default]]
 *  (raw optic names, default deriver) for direct non-`Table` use in tests.
 *
 *  Path-resolution failures become [[UpdateExpression.Action.Failure]] nodes, surfaced at
 *  query execution by the interpreter's `validateAction` pass — the same treatment
 *  `ConditionExpression.Failure` gets.
 */
private[dynamodb] object DdbUpdateExprInterpreter {

  def toAction[From](expr: DdbUpdateExpr[From]): Action[From] =
    interp(expr, ExprCtx.default)

  def toAction[From](expr: DdbUpdateExpr[From], ctx: ExprCtx): Action[From] =
    interp(expr, ctx)

  /**
   * Overload for callers that hold a config + reflect rather than a `Table`'s `ExprCtx`
   *  (tests, and any direct non-`Table` use). Allocates a one-off `ExprCtx`; the hot
   *  `DdbExprApi.update` path goes through `toAction(expr, table.exprCtx)`.
   */
  def toAction[From](
    expr: DdbUpdateExpr[From],
    config: DynamoDBCodecDeriverConfig[From],
    rootReflect: Reflect[Binding, From]
  ): Action[From] = {
    val root     = new Schema(rootReflect).deriving(config.toResolverDeriver).derive
    val resolver = new ProjectionResolver(root)
    interp(expr, new ExprCtx(config, resolver))
  }

  private def interp[From](expr: DdbUpdateExpr[From], ctx: ExprCtx): Action[From] =
    flatten(expr).map(leaf(_, ctx)) match {
      case Nil           => Action.Failure("empty update expression")
      case single :: Nil => single
      case many          => many.reduceLeft[Action[From]]((acc, r) => acc + r)
    }

  private def flatten[From](e: DdbUpdateExpr[From]): List[DdbUpdateExpr[From]] =
    e match {
      case DdbUpdateExpr.Combine(l, r) => flatten(l) ::: flatten(r)
      case other                       => other :: Nil
    }

  private def leaf[From](e: DdbUpdateExpr[From], ctx: ExprCtx): UpdateExpression.RenderableAction[From] = {
    def peA[A](optic: Optic[_, _]): Either[String, ProjectionExpression[From, A]] =
      ctx.peOf(optic).map(_.asInstanceOf[ProjectionExpression[From, A]])

    e match {
      case DdbUpdateExpr.SetValue(optic, value, schema)   =>
        peA(optic).fold(Action.Failure(_), p => SetAction(p, SetOperand.ValueOperand(ctx.encode(value, schema))))
      case DdbUpdateExpr.SetPath(optic, other)            =>
        (peA(optic), peA(other)) match {
          case (Right(p), Right(op)) => SetAction(p, SetOperand.PathOperand(op))
          case (Left(msg), _)        => Action.Failure(msg)
          case (_, Left(msg))        => Action.Failure(msg)
        }
      case DdbUpdateExpr.SetIfNotExists(optic, v, schema) =>
        peA(optic).fold(Action.Failure(_), p => SetAction(p, SetOperand.IfNotExists(p, ctx.encode(v, schema))))
      case DdbUpdateExpr.Remove(optic)                    =>
        peA(optic).fold(Action.Failure(_), p => RemoveAction(p))
      case DdbUpdateExpr.RemoveAt(optic, index)           =>
        peA(optic).fold(Action.Failure(_), p => RemoveAction(ProjectionExpression.ListElement(p, index)))
      case DdbUpdateExpr.Increment(optic, delta, schema)  =>
        peA(optic).fold(
          Action.Failure(_),
          p => SetAction(p, SetOperand.PathOperand(p) + SetOperand.ValueOperand(ctx.encode(delta, schema)))
        )
      case DdbUpdateExpr.Decrement(optic, delta, schema)  =>
        peA(optic).fold(
          Action.Failure(_),
          p => SetAction(p, SetOperand.PathOperand(p) - SetOperand.ValueOperand(ctx.encode(delta, schema)))
        )
      case DdbUpdateExpr.Add(optic, value, schema)        =>
        peA(optic).fold(Action.Failure(_), p => AddAction(p, ctx.encode(value, schema)))
      case DdbUpdateExpr.AddToSet(optic, value, schema)   =>
        peA(optic).fold(Action.Failure(_), p => AddAction(p, ctx.encode(value, schema)))
      case DdbUpdateExpr.DeleteFromSet(optic, v, schema)  =>
        peA(optic).fold(Action.Failure(_), p => DeleteAction(p, ctx.encode(v, schema)))
      case DdbUpdateExpr.AppendList(optic, items, es)     =>
        peA(optic).fold(
          Action.Failure(_),
          p => SetAction(p, SetOperand.ListAppend(p, AttributeValue.List(encodeAll(items, es, ctx))))
        )
      case DdbUpdateExpr.PrependList(optic, items, es)    =>
        peA(optic).fold(
          Action.Failure(_),
          p => SetAction(p, SetOperand.ListPrepend(p, AttributeValue.List(encodeAll(items, es, ctx))))
        )
      case DdbUpdateExpr.Failure(message)                 =>
        Action.Failure(message)
      case _: DdbUpdateExpr.Combine[From]                 =>
        Action.Failure("internal: unflattened Combine") // unreachable — flatten strips Combine nodes
    }
  }

  private def encodeAll[B](items: Seq[B], schema: Schema[B], ctx: ExprCtx): List[AttributeValue] =
    items.map(i => ctx.encode(i, schema)).toList
}
