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

import zio.blocks.schema.Optic
import zio.dynamodb.blocks.OpticToPE
import zio.dynamodb.{ AttributeValue, KeyConditionExpr, PartitionKey, ProjectionExpression, SortKey }

/**
 * Interprets a [[DdbKeyExpr]][S] into a [[KeyConditionExpr]][S].
 *
 *  Field references are resolved via [[OpticToPE]], then mapped to their DynamoDB
 *  attribute name through the calling table's `recordFieldNameMap` (so a configured
 *  field-name mapper / `@Modifier.rename` is honoured). Literal values are encoded with
 *  the calling table's configured codec. Both come off the table's [[ExprCtx]], which
 *  memoises them; the no-config overloads use the shared [[ExprCtx.default]] (raw optic
 *  names, default deriver) for the low-level `.whereKey` conversion path.
 *
 *  Only single-segment optics (top-level fields) are valid as partition or sort keys; a
 *  multi-segment path returns a Left with a descriptive message.
 */
object DdbKeyExprInterpreter {

  // -- PrimaryKey -------------------------------------------------------------

  def toPrimaryKeyExpr[S](expr: DdbKeyExpr.PrimaryKey[S]): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    toPrimaryKeyExpr(expr, ExprCtx.default)

  def toPrimaryKeyExpr[S](
    expr: DdbKeyExpr.PrimaryKey[S],
    ctx: ExprCtx
  ): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    expr match {
      case DdbKeyExpr.PartitionKeyEquals(optic, value, schema) =>
        fieldName(optic, ctx).map { name =>
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](name), ctx.encode(value, schema))
        }
      case DdbKeyExpr.Composite(pkExpr, skEq)                  =>
        for {
          pkName <- fieldName(pkExpr.optic, ctx)
          skName <- fieldName(skEq.optic, ctx)
        } yield {
          val pkNode = KeyConditionExpr.PartitionKeyEquals[S](
            PartitionKey[S, Any](pkName),
            ctx.encode(pkExpr.value, pkExpr.schema)
          )
          val skNode =
            KeyConditionExpr.SortKeyEquals[S](SortKey[S, Any](skName), ctx.encode(skEq.value, skEq.schema))
          KeyConditionExpr.CompositePrimaryKeyExpr[S](pkNode, skNode)
        }
    }

  // -- Full DdbKeyExpr (adds Extended) --------------------------------------

  def toKeyConditionExpr[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    toKeyConditionExpr(expr, ExprCtx.default)

  def toKeyConditionExpr[S](expr: DdbKeyExpr[S], ctx: ExprCtx): Either[String, KeyConditionExpr[S]] =
    expr match {
      case pk: DdbKeyExpr.PrimaryKey[S]       =>
        toPrimaryKeyExpr(pk, ctx)
      case DdbKeyExpr.Extended(pkExpr, skExt) =>
        for {
          pkName <- fieldName(pkExpr.optic, ctx)
          skNode <- toExtendedSortKey[S](skExt, ctx)
        } yield KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[S](
          KeyConditionExpr.PartitionKeyEquals[S](
            PartitionKey[S, Any](pkName),
            ctx.encode(pkExpr.value, pkExpr.schema)
          ),
          skNode
        )
    }

  // -- Helpers -------------------------------------------------------------

  // Only top-level field optics are valid as DynamoDB key fields; map the raw Scala name
  // to the wire name the calling table's codec configuration produced.
  private def fieldName[S, A](optic: Optic[S, A], ctx: ExprCtx): Either[String, String] =
    OpticToPE.pe(optic) match {
      case Right(ProjectionExpression.MapElement(ProjectionExpression.Root, key)) =>
        Right(ctx.recordFieldNameMap.getOrElse(key, key))
      case Right(pe)                                                              =>
        Left(s"key field must be a single top-level field, got path: $pe")
      case Left(error)                                                            =>
        Left(error)
    }

  private def toExtendedSortKey[S](
    sortKey: DdbKeyExpr.SortKeyExtended[S],
    ctx: ExprCtx
  ): Either[String, KeyConditionExpr.ExtendedSortKeyExpr[S, _]] =
    sortKey match {
      case DdbKeyExpr.SortKeyExtended.Gt(optic, value, schema)       =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThan(SortKey[S, Any](n), ctx.encode(value, schema))
        )
      case DdbKeyExpr.SortKeyExtended.Gte(optic, value, schema)      =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThanOrEqual(SortKey[S, Any](n), ctx.encode(value, schema))
        )
      case DdbKeyExpr.SortKeyExtended.Lt(optic, value, schema)       =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThan(SortKey[S, Any](n), ctx.encode(value, schema))
        )
      case DdbKeyExpr.SortKeyExtended.Lte(optic, value, schema)      =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThanOrEqual(SortKey[S, Any](n), ctx.encode(value, schema))
        )
      case DdbKeyExpr.SortKeyExtended.Between(optic, lo, hi, schema) =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr
            .Between(SortKey[S, Any](n), ctx.encode(lo, schema), ctx.encode(hi, schema))
        )
      case DdbKeyExpr.SortKeyExtended.BeginsWith(optic, prefix)      =>
        fieldName(optic, ctx).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.BeginsWith(SortKey[S, Any](n), AttributeValue.String(prefix))
        )
    }
}
