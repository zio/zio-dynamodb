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

import zio.blocks.schema.{ DynamicSchemaExpr, DynamicValue, Reflect, Schema }
import zio.blocks.schema.binding.Binding
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver
import zio.dynamodb.{ AttributeValue, ConditionExpression, ProjectionExpression }

/**
 * Interprets a [[DdbExpr]][S, Boolean] into a [[ConditionExpression]][S].
 *
 *  DDB function nodes ([[DdbExpr.Between]], [[DdbExpr.In]], etc.) and [[DdbExpr.Builtin]]
 *  (scalar / sealed-trait comparisons via [[zio.blocks.schema.SchemaExpr]]) carry a
 *  [[Schema]] for the literal; the codec is derived at evaluation time.
 *
 *  Field paths and literals are resolved through an [[ExprCtx]] - per-[[Table]] on the
 *  configured path (so `.filter` / `.where` see the same attribute names and encoding rules
 *  as the item body), or the shared [[ExprCtx.default]] (raw optic names, default deriver)
 *  for the low-level implicit-conversion path. The `ExprCtx` memoises resolved projections
 *  and literal codecs, so construction allocates neither cache keys nor a context object.
 */
object DdbExprInterpreter {

  def toConditionExpression[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    interp[S](expr, ExprCtx.default)

  def toConditionExpression[S](expr: DdbExpr[S, Boolean], ctx: ExprCtx): Either[String, ConditionExpression[S]] =
    interp[S](expr, ctx)

  /**
   * Overload for callers that hold a config + reflect rather than a `Table`'s `ExprCtx`
   *  (tests, and any direct non-`Table` use). Allocates a one-off `ExprCtx`; the hot
   *  `.where` / `.filter` path goes through `toConditionExpression(expr, table.exprCtx)`.
   */
  def toConditionExpression[S](
    expr: DdbExpr[S, Boolean],
    config: DynamoDBCodecDeriverConfigure[S],
    rootReflect: Reflect[Binding, S]
  ): Either[String, ConditionExpression[S]] =
    interp[S](expr, new ExprCtx(config, rootReflect, Map.empty))

  private def interp[S](expr: DdbExpr[S, Boolean], ctx: ExprCtx): Either[String, ConditionExpression[S]] =
    expr match {

      case DdbExpr.Builtin(se) =>
        Right(fromDynamicSchemaExpr[S](se.dynamic, ctx))

      case DdbExpr.And(l, r) =>
        for { lce <- interp(l, ctx); rce <- interp(r, ctx) } yield ConditionExpression.And(lce, rce)

      case DdbExpr.Or(l, r) =>
        for { lce <- interp(l, ctx); rce <- interp(r, ctx) } yield ConditionExpression.Or(lce, rce)

      case DdbExpr.Not(inner) =>
        interp(inner, ctx).map(ConditionExpression.Not(_))

      case DdbExpr.AttributeExists(optic) =>
        ctx.peOf(optic).map(p => ConditionExpression.AttributeExists(p.asInstanceOf[ProjectionExpression[S, Any]]))

      case DdbExpr.AttributeNotExists(optic) =>
        ctx.peOf(optic).map(p => ConditionExpression.AttributeNotExists(p.asInstanceOf[ProjectionExpression[S, Any]]))

      case DdbExpr.Between(optic, lo, hi, schema) =>
        ctx.peOf(optic).map { p =>
          ConditionExpression.Between(
            ConditionExpression.Operand.ProjectionExpressionOperand(p.asInstanceOf[ProjectionExpression[S, Any]]),
            ctx.encode(lo, schema),
            ctx.encode(hi, schema)
          )
        }

      case DdbExpr.In(optic, values, schema) =>
        ctx.peOf(optic).map { p =>
          ConditionExpression.In(
            ConditionExpression.Operand.ProjectionExpressionOperand(p.asInstanceOf[ProjectionExpression[S, Any]]),
            values.map(v => ctx.encode(v, schema)).toSet
          )
        }

      case DdbExpr.BeginsWith(optic, prefix) =>
        ctx.peOf(optic).map { p =>
          ConditionExpression.BeginsWith(p.asInstanceOf[ProjectionExpression[S, Any]], AttributeValue.String(prefix))
        }

      case DdbExpr.Contains(optic, value) =>
        ctx.peOf(optic).map { p =>
          ConditionExpression.Contains(p.asInstanceOf[ProjectionExpression[S, Any]], AttributeValue.String(value))
        }

      case elem: DdbExpr.ContainsElement[S, _, b] =>
        ctx.peOf(elem.optic).map { p =>
          ConditionExpression.Contains(
            p.asInstanceOf[ProjectionExpression[S, Any]],
            ctx.encode[b](elem.element, elem.elemSchema)
          )
        }
    }

  // Folds a DynamicSchemaExpr (from zio-blocks-schema) into a ConditionExpression directly.
  private def fromDynamicSchemaExpr[A](dse: DynamicSchemaExpr, ctx: ExprCtx): ConditionExpression[A] = {

    def toRelational(
      l: ConditionExpression.Operand[A, _],
      r: ConditionExpression.Operand[A, _],
      op: DynamicSchemaExpr.RelationalOperator
    ): ConditionExpression[A] =
      op match {
        case DynamicSchemaExpr.RelationalOperator.Equal              => ConditionExpression.Equals(l, r)
        case DynamicSchemaExpr.RelationalOperator.NotEqual           => ConditionExpression.NotEqual(l, r)
        case DynamicSchemaExpr.RelationalOperator.LessThan           => ConditionExpression.LessThan(l, r)
        case DynamicSchemaExpr.RelationalOperator.LessThanOrEqual    => ConditionExpression.LessThanOrEqual(l, r)
        case DynamicSchemaExpr.RelationalOperator.GreaterThan        => ConditionExpression.GreaterThan(l, r)
        case DynamicSchemaExpr.RelationalOperator.GreaterThanOrEqual => ConditionExpression.GreaterThanOrEqual(l, r)
      }

    dse match {
      case DynamicSchemaExpr.Relational(
            DynamicSchemaExpr.Select(path),
            DynamicSchemaExpr.Literal(dynValue, schema),
            op
          ) =>
        ctx.peOf(path) match {
          case Left(msg) => ConditionExpression.Failure(msg)
          case Right(p)  =>
            val peOpd  = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](p.asInstanceOf[ProjectionExpression[A, Any]])
            val valOpd = ConditionExpression.Operand
              .ValueOperand[A](encodeFromSchema(dynValue, schema, ctx))
            toRelational(peOpd, valOpd, op)
        }

      case DynamicSchemaExpr.Relational(DynamicSchemaExpr.Select(l), DynamicSchemaExpr.Select(r), op) =>
        (ctx.peOf(l), ctx.peOf(r)) match {
          case (Right(lpe), Right(rpe)) =>
            val lOpd = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](lpe.asInstanceOf[ProjectionExpression[A, Any]])
            val rOpd = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](rpe.asInstanceOf[ProjectionExpression[A, Any]])
            toRelational(lOpd, rOpd, op)
          case (Left(msg), _)           => ConditionExpression.Failure(msg)
          case (_, Left(msg))           => ConditionExpression.Failure(msg)
        }

      case DynamicSchemaExpr.Logical(left, right, op) =>
        val leftCE  = fromDynamicSchemaExpr[A](left, ctx)
        val rightCE = fromDynamicSchemaExpr[A](right, ctx)
        op match {
          case DynamicSchemaExpr.LogicalOperator.And => ConditionExpression.And(leftCE, rightCE)
          case DynamicSchemaExpr.LogicalOperator.Or  => ConditionExpression.Or(leftCE, rightCE)
        }

      case DynamicSchemaExpr.Not(inner) =>
        ConditionExpression.Not(fromDynamicSchemaExpr[A](inner, ctx))

      case other =>
        ConditionExpression.Failure(s"unexpected DynamicSchemaExpr: $other")
    }
  }

  private def encodeFromSchema(dv: DynamicValue, schema: Schema[_], ctx: ExprCtx): AttributeValue = {
    val s = schema.asInstanceOf[Schema[Any]]
    s.fromDynamicValue(dv)
      .fold(
        _ => DynamoDBCodecDeriver.dynamicValueCodec.encoder(dv),
        a => ctx.encode[Any](a, s)
      )
  }
}
