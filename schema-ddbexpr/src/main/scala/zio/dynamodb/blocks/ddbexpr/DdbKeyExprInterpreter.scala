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

import zio.blocks.schema.{ DynamicOptic, Optic, Reflect, Schema }
import zio.blocks.schema.binding.Binding
import zio.dynamodb.blocks.schema.DynamoDBCodec
import zio.dynamodb.{ AttributeValue, KeyConditionExpr, PartitionKey, ProjectionExpression, SortKey }

/**
 * Interprets a [[DdbKeyExpr]][S] into a [[KeyConditionExpr]][S].
 *
 *  Field references and literal values are both resolved against the calling table's codec
 *  through its [[ExprCtx]], as `.where` / `.filter` are, so key conditions, filters and the
 *  stored item agree on names and encodings by construction.
 *
 *  Only single-segment optics (top-level fields) are valid as partition or sort keys; a
 *  multi-segment path returns a Left with a descriptive message.
 */
private[dynamodb] object DdbKeyExprInterpreter {

  // -- PrimaryKey -------------------------------------------------------------

  def toPrimaryKeyExpr[S](
    expr: DdbKeyExpr.PrimaryKey[S],
    ctx: ExprCtx
  ): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    expr match {
      case DdbKeyExpr.PartitionKeyEquals(optic, value, _) =>
        keyField(optic, ctx).map { pk =>
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](keyName(pk)), pk.codec.encoder(value))
        }
      case DdbKeyExpr.Composite(pkExpr, skEq)             =>
        for {
          pk <- keyField(pkExpr.optic, ctx)
          sk <- keyField(skEq.optic, ctx)
        } yield {
          val pkNode =
            KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](keyName(pk)), pk.codec.encoder(pkExpr.value))
          val skNode = KeyConditionExpr.SortKeyEquals[S](SortKey[S, Any](keyName(sk)), sk.codec.encoder(skEq.value))
          KeyConditionExpr.CompositePrimaryKeyExpr[S](pkNode, skNode)
        }
    }

  // -- Full DdbKeyExpr (adds KeyRange) --------------------------------------

  def toKeyConditionExpr[S](expr: DdbKeyExpr[S], ctx: ExprCtx): Either[String, KeyConditionExpr[S]] =
    expr match {
      case pk: DdbKeyExpr.PrimaryKey[S]         =>
        toPrimaryKeyExpr(pk, ctx)
      case DdbKeyExpr.KeyRange(pkExpr, skRange) =>
        for {
          pk     <- keyField(pkExpr.optic, ctx)
          skNode <- toSortKeyRange[S](skRange, ctx)
        } yield KeyConditionExpr.KeyRangeExpr[S](
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](keyName(pk)), pk.codec.encoder(pkExpr.value)),
          skNode
        )
    }

  // For callers without a `Table` (tests); allocates a one-off `ExprCtx`, unlike the `Table` path.
  def toKeyConditionExpr[S](expr: DdbKeyExpr[S], codec: DynamoDBCodec[S]): Either[String, KeyConditionExpr[S]] =
    toKeyConditionExpr(expr, new ExprCtx(codec))

  // -- Helpers -------------------------------------------------------------

  // A key optic's step: its attribute name (always a `Name` segment) and the codec its value encodes
  // with. A valid key optic is a single top-level field, so the common case returns the table codec's
  // prebuilt step as is - no allocation. Anything else resolves the full path and is rejected unless it
  // came out as a single top-level attribute.
  private def keyField[S, A](optic: Optic[S, A], ctx: ExprCtx): Either[String, DynamoDBCodec.Step] = {
    val nodes = optic.toDynamic.nodes
    if (nodes.length == 1)
      nodes.head match {
        case field: DynamicOptic.Node.Field =>
          val step = ctx.topLevelField(field)
          step match {
            case Right(st) if !st.segment.isInstanceOf[DynamoDBCodec.Segment.Name] =>
              Left(s"key field must be a single top-level field, got: $optic")
            case _                                                                 => step
          }
        case _                              => fallback(optic, ctx)
      }
    else fallback(optic, ctx)
  }

  private def fallback[S, A](optic: Optic[S, A], ctx: ExprCtx): Either[String, DynamoDBCodec.Step] =
    ctx.at(optic) match {
      case Right(r)    =>
        r.path match {
          case ProjectionExpression.MapElement(ProjectionExpression.Root, key) =>
            DynamoDBCodec.stepTo(DynamoDBCodec.Segment.Name(key), r.codec)
          case pe                                                              =>
            Left(s"key field must be a single top-level field, got path: $pe")
        }
      case Left(error) => Left(error)
    }

  private def keyName(step: DynamoDBCodec.Step): String =
    step.segment match {
      case DynamoDBCodec.Segment.Name(name) => name
      case other                            => other.toString // unreachable: keyField only returns Name steps
    }

  private def toSortKeyRange[S](
    sortKey: DdbKeyExpr.SortKeyRange[S],
    ctx: ExprCtx
  ): Either[String, KeyConditionExpr.SortKeyRange[S, _]] =
    sortKey match {
      case DdbKeyExpr.SortKeyRange.Gt(optic, value, _)       =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange.GreaterThan(SortKey[S, Any](keyName(sk)), sk.codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyRange.Gte(optic, value, _)      =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange.GreaterThanOrEqual(SortKey[S, Any](keyName(sk)), sk.codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyRange.Lt(optic, value, _)       =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange.LessThan(SortKey[S, Any](keyName(sk)), sk.codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyRange.Lte(optic, value, _)      =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange.LessThanOrEqual(SortKey[S, Any](keyName(sk)), sk.codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyRange.Between(optic, lo, hi, _) =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange
            .Between(SortKey[S, Any](keyName(sk)), sk.codec.encoder(lo), sk.codec.encoder(hi))
        )
      case DdbKeyExpr.SortKeyRange.BeginsWith(optic, prefix) =>
        keyField(optic, ctx).map(sk =>
          KeyConditionExpr.SortKeyRange.BeginsWith(SortKey[S, Any](keyName(sk)), AttributeValue.String(prefix))
        )
    }
}
