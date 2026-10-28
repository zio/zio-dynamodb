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
import zio.dynamodb.blocks.schema.DynamoDBCodec
import zio.dynamodb.{ AttributeValue, ProjectionExpression, UpdateExpression }
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.Action.{ AddAction, DeleteAction, RemoveAction, SetAction }
import zio.dynamodb.UpdateExpression.SetOperand

/**
 * Interprets a [[DdbUpdateExpr]][From] into `core`'s [[UpdateExpression.Action]][From] — the
 *  update-action counterpart of [[DdbExprInterpreter]].
 *
 *  Attribute paths are resolved and operand literals encoded through an [[ExprCtx]],
 *  per-[[Table]] on the configured path, so `update` writes the same attribute names and
 *  literal encodings as `put` does for the item body.
 *
 *  Path-resolution failures become [[UpdateExpression.Action.Failure]] nodes, surfaced at
 *  query execution by the interpreter's `validateAction` pass — the same treatment
 *  `ConditionExpression.Failure` gets.
 */
private[dynamodb] object DdbUpdateExprInterpreter {

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
  ): Action[From] =
    interp(expr, new ExprCtx(new Schema(rootReflect).deriving(config.toDeriver).derive))

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
    // The attribute path and the codec that writes that position, from one lookup on the table's codec.
    def at[A](
      optic: Optic[_, _]
    )(f: (ProjectionExpression[From, A], DynamoDBCodec[Any]) => UpdateExpression.RenderableAction[From]) =
      ctx.at(optic) match {
        case Right(r)  => f(r.path.asInstanceOf[ProjectionExpression[From, A]], r.codec)
        case Left(msg) => Action.Failure[From](msg)
      }
    def items[A](
      optic: Optic[_, _]
    )(f: (ProjectionExpression[From, A], DynamoDBCodec[Any]) => UpdateExpression.RenderableAction[From]) =
      ctx.at(optic).flatMap(r => ctx.elementCodec(r).map(c => (r, c))) match {
        case Right((r, elem)) => f(r.path.asInstanceOf[ProjectionExpression[From, A]], elem)
        case Left(msg)        => Action.Failure[From](msg)
      }

    e match {
      case DdbUpdateExpr.SetValue(optic, value, _)   =>
        at(optic)((p, c) => SetAction(p, SetOperand.ValueOperand(c.encoder(value))))
      case DdbUpdateExpr.SetPath(optic, other)       =>
        (ctx.at(optic), ctx.at(other)) match {
          case (Right(p), Right(op)) =>
            SetAction(
              p.path.asInstanceOf[ProjectionExpression[From, Any]],
              SetOperand.PathOperand(op.path.asInstanceOf[ProjectionExpression[From, Any]])
            )
          case (Left(msg), _)        => Action.Failure(msg)
          case (_, Left(msg))        => Action.Failure(msg)
        }
      case DdbUpdateExpr.SetIfNotExists(optic, v, _) =>
        at(optic)((p, c) => SetAction(p, SetOperand.IfNotExists(p, c.encoder(v))))
      case DdbUpdateExpr.Remove(optic)               =>
        at(optic)((p, _) => RemoveAction(p))
      case DdbUpdateExpr.RemoveAt(optic, index)      =>
        at(optic)((p, _) => RemoveAction(ProjectionExpression.ListElement(p, index)))
      case DdbUpdateExpr.Increment(optic, delta, _)  =>
        at(optic)((p, c) => SetAction(p, SetOperand.PathOperand(p) + SetOperand.ValueOperand(c.encoder(delta))))
      case DdbUpdateExpr.Decrement(optic, delta, _)  =>
        at(optic)((p, c) => SetAction(p, SetOperand.PathOperand(p) - SetOperand.ValueOperand(c.encoder(delta))))
      case DdbUpdateExpr.Add(optic, value, _)        =>
        at(optic)((p, c) => AddAction(p, c.encoder(value)))
      case DdbUpdateExpr.AddToSet(optic, value, _)   =>
        at(optic)((p, c) => AddAction(p, c.encoder(value)))
      case DdbUpdateExpr.DeleteFromSet(optic, v, _)  =>
        at(optic)((p, c) => DeleteAction(p, c.encoder(v)))
      case DdbUpdateExpr.AppendList(optic, xs, _)    =>
        items(optic)((p, c) => SetAction(p, SetOperand.ListAppend(p, AttributeValue.List(xs.map(c.encoder).toList))))
      case DdbUpdateExpr.PrependList(optic, xs, _)   =>
        items(optic)((p, c) => SetAction(p, SetOperand.ListPrepend(p, AttributeValue.List(xs.map(c.encoder).toList))))
      case _: DdbUpdateExpr.Combine[From]            =>
        Action.Failure("internal: unflattened Combine") // unreachable — flatten strips Combine nodes
    }
  }
}
