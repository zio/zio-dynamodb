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
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.{ AttributeValue, ConditionExpression, ProjectionExpression }

/**
 * Interprets a [[DdbExpr]][S, Boolean] into a [[ConditionExpression]][S].
 *
 *  DDB function nodes ([[DdbExpr.Between]], [[DdbExpr.In]], etc.) and [[DdbExpr.Builtin]]
 *  (scalar / sealed-trait comparisons via [[zio.blocks.schema.SchemaExpr]]) carry a
 *  [[Schema]] for the literal; the codec is derived at evaluation time.
 *
 *  Field paths and literals are resolved through an [[ExprCtx]] - per-[[Table]] on the
 *  configured path, so `.filter` / `.where` see the same attribute names and encoding
 *  rules as the item body. The `ExprCtx` memoises resolved projections and literal
 *  codecs, so construction allocates neither cache keys nor a context object.
 */
private[dynamodb] object DdbExprInterpreter {

  def toConditionExpression[S](expr: DdbExpr[S, Boolean], ctx: ExprCtx): Either[String, ConditionExpression[S]] =
    interp[S](expr, ctx)

  // For callers without a `Table` (tests); allocates a one-off `ExprCtx`, unlike the `Table` path.
  def toConditionExpression[S](
    expr: DdbExpr[S, Boolean],
    codec: DynamoDBCodec[S]
  ): Either[String, ConditionExpression[S]] =
    interp[S](expr, new ExprCtx(codec))

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
        ctx.at(optic).map(r => ConditionExpression.AttributeExists(r.path.asInstanceOf[ProjectionExpression[S, Any]]))

      case DdbExpr.AttributeNotExists(optic) =>
        ctx
          .at(optic)
          .map(r => ConditionExpression.AttributeNotExists(r.path.asInstanceOf[ProjectionExpression[S, Any]]))

      case DdbExpr.Between(optic, lo, hi, _) =>
        ctx.at(optic).map { r =>
          ConditionExpression.Between(
            ConditionExpression.Operand.ProjectionExpressionOperand(r.path.asInstanceOf[ProjectionExpression[S, Any]]),
            r.codec.encoder(lo),
            r.codec.encoder(hi)
          )
        }

      case DdbExpr.In(optic, values, _) =>
        ctx.at(optic).map { r =>
          ConditionExpression.In(
            ConditionExpression.Operand.ProjectionExpressionOperand(r.path.asInstanceOf[ProjectionExpression[S, Any]]),
            values.map(v => r.codec.encoder(v)).toSet
          )
        }

      case DdbExpr.BeginsWith(optic, prefix) =>
        ctx.at(optic).map { r =>
          val p = r.path
          ConditionExpression.BeginsWith(p.asInstanceOf[ProjectionExpression[S, Any]], AttributeValue.String(prefix))
        }

      case DdbExpr.Contains(optic, value) =>
        ctx.at(optic).map { r =>
          val p = r.path
          ConditionExpression.Contains(p.asInstanceOf[ProjectionExpression[S, Any]], AttributeValue.String(value))
        }

      case DdbExpr.SizeCompare(optic, op, value) =>
        ctx.at(optic).map { r =>
          val p = r.path
          toRelational[S](
            ConditionExpression.Operand.Size(p.asInstanceOf[ProjectionExpression[S, Any]]),
            ConditionExpression.Operand.ValueOperand[S](AttributeValue.Number(BigDecimal(value))),
            op
          )
        }

      case DdbExpr.SizeBetween(optic, lo, hi) =>
        ctx.at(optic).map { r =>
          val p = r.path
          ConditionExpression.Between(
            ConditionExpression.Operand.Size(p.asInstanceOf[ProjectionExpression[S, Any]]),
            AttributeValue.Number(BigDecimal(lo)),
            AttributeValue.Number(BigDecimal(hi))
          )
        }

      case elem: DdbExpr.ContainsElement[S, _, b] =>
        for {
          r     <- ctx.at(elem.optic)
          codec <- ctx.elementCodec(r)
        } yield ConditionExpression.Contains(
          r.path.asInstanceOf[ProjectionExpression[S, Any]],
          codec.encoder(elem.element)
        )
    }

  private def toRelational[A](
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

  // Folds a DynamicSchemaExpr (from zio-blocks-schema) into a ConditionExpression directly.
  private def fromDynamicSchemaExpr[A](dse: DynamicSchemaExpr, ctx: ExprCtx): ConditionExpression[A] =

    dse match {
      case DynamicSchemaExpr.Relational(
            DynamicSchemaExpr.Select(path),
            DynamicSchemaExpr.Literal(dynValue, schema),
            op
          ) =>
        ctx.at(path) match {
          case Left(msg) => ConditionExpression.Failure(msg)
          case Right(r)  =>
            val peOpd  = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](r.path.asInstanceOf[ProjectionExpression[A, Any]])
            val valOpd = ConditionExpression.Operand
              .ValueOperand[A](encodeFromSchema(dynValue, schema, r.codec))
            toRelational[A](peOpd, valOpd, op)
        }

      case DynamicSchemaExpr.Relational(DynamicSchemaExpr.Select(l), DynamicSchemaExpr.Select(r), op) =>
        (ctx.at(l), ctx.at(r)) match {
          case (Right(lr), Right(rr)) =>
            val lOpd = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](lr.path.asInstanceOf[ProjectionExpression[A, Any]])
            val rOpd = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](rr.path.asInstanceOf[ProjectionExpression[A, Any]])
            toRelational[A](lOpd, rOpd, op)
          case (Left(msg), _)         => ConditionExpression.Failure(msg)
          case (_, Left(msg))         => ConditionExpression.Failure(msg)
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

  private def encodeFromSchema(dv: DynamicValue, schema: Schema[_], codec: DynamoDBCodec[Any]): AttributeValue =
    schema
      .asInstanceOf[Schema[Any]]
      .fromDynamicValue(dv)
      .fold(_ => DynamoDBCodecDeriver.dynamicValueCodec.encoder(dv), codec.encoder(_))
}
