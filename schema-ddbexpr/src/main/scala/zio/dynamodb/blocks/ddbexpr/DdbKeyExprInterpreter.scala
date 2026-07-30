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
 *  Field references are resolved via [[OpticToPE]]. Only single-segment optics
 *  (top-level fields) are valid as partition or sort keys; a multi-segment path
 *  returns a Left with a descriptive message.
 */
object DdbKeyExprInterpreter {

  // PrimaryKey covers PartitionKeyEquals and Composite — the only valid key forms for
  // get/update/deleteFrom. Extended is not a PrimaryKey, so range expressions are
  // rejected at compile time.
  def toPrimaryKeyExpr[S](expr: DdbKeyExpr.PrimaryKey[S]): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    expr match {
      case DdbKeyExpr.PartitionKeyEquals(optic, value, codec) =>
        fieldName(optic).map { name =>
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](name), codec.encoder(value))
        }
      case DdbKeyExpr.Composite(pkExpr, skEq)                 =>
        for {
          pkName <- fieldName(pkExpr.optic)
          skName <- fieldName(skEq.optic)
        } yield {
          val pkNode =
            KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](pkName), pkExpr.codec.encoder(pkExpr.value))
          val skNode = KeyConditionExpr.SortKeyEquals[S](SortKey[S, Any](skName), skEq.codec.encoder(skEq.value))
          KeyConditionExpr.CompositePrimaryKeyExpr[S](pkNode, skNode)
        }
    }

  def toKeyConditionExpr[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    expr match {
      case pk: DdbKeyExpr.PrimaryKey[S]       =>
        toPrimaryKeyExpr(pk)
      case DdbKeyExpr.Extended(pkExpr, skExt) =>
        for {
          pkName <- fieldName(pkExpr.optic)
          skNode <- toExtendedSortKey[S](skExt)
        } yield KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[S](
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](pkName), pkExpr.codec.encoder(pkExpr.value)),
          skNode
        )
    }

  // Only top-level field optics are valid as DynamoDB key fields.
  private def fieldName[S, A](optic: Optic[S, A]): Either[String, String] =
    OpticToPE.pe(optic) match {
      case Right(ProjectionExpression.MapElement(ProjectionExpression.Root, key)) => Right(key)
      case Right(pe)                                                              => Left(s"key field must be a single top-level field, got path: $pe")
      case Left(error)                                                            => Left(error)
    }

  private def toExtendedSortKey[S](
    sortKey: DdbKeyExpr.SortKeyExtended[S]
  ): Either[String, KeyConditionExpr.ExtendedSortKeyExpr[S, _]] =
    sortKey match {
      case DdbKeyExpr.SortKeyExtended.Gt(optic, value, codec)       =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThan(SortKey[S, Any](n), codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyExtended.Gte(optic, value, codec)      =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThanOrEqual(SortKey[S, Any](n), codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyExtended.Lt(optic, value, codec)       =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThan(SortKey[S, Any](n), codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyExtended.Lte(optic, value, codec)      =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThanOrEqual(SortKey[S, Any](n), codec.encoder(value))
        )
      case DdbKeyExpr.SortKeyExtended.Between(optic, lo, hi, codec) =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.Between(SortKey[S, Any](n), codec.encoder(lo), codec.encoder(hi))
        )
      case DdbKeyExpr.SortKeyExtended.BeginsWith(optic, prefix)     =>
        fieldName(optic).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.BeginsWith(SortKey[S, Any](n), AttributeValue.String(prefix))
        )
    }
}
