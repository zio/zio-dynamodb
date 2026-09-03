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

import java.util.concurrent.ConcurrentHashMap
import zio.blocks.schema.{ Optic, Schema }
import zio.dynamodb.blocks.DynamoDBCodecDeriverConfigure
import zio.dynamodb.blocks.OpticToPE
import zio.dynamodb.blocks.schema.DynamoDBCodec
import zio.dynamodb.{ AttributeValue, KeyConditionExpr, PartitionKey, ProjectionExpression, SortKey }

/**
 * Interprets a [[DdbKeyExpr]][S] into a [[KeyConditionExpr]][S].
 *
 *  Field references are resolved via [[OpticToPE]], then mapped to their DynamoDB
 *  attribute name through the calling table's `recordFieldNameMap` (so a configured
 *  field-name mapper / `@Modifier.rename` is honoured). Literal values are encoded with
 *  a codec derived from the calling table's [[DynamoDBCodecDeriverConfigure]]. The
 *  no-config overloads keep the old behaviour (raw optic names, default deriver) for the
 *  LL `.whereKey` conversion path.
 *
 *  Only single-segment optics (top-level fields) are valid as partition or sort keys; a
 *  multi-segment path returns a Left with a descriptive message.
 */
object DdbKeyExprInterpreter {

  // Memoises value-literal codecs by (Schema, config) — config compares by value.
  private val valueCodecCache = new ConcurrentHashMap[CodecCacheKey, DynamoDBCodec[_]]()

  private def encode[A](value: A, schema: Schema[A], config: DynamoDBCodecDeriverConfigure[_]): AttributeValue =
    valueCodecCache
      .computeIfAbsent(new CodecCacheKey(schema, config), _ => schema.deriving(config.toDeriver).derive)
      .asInstanceOf[DynamoDBCodec[A]]
      .encoder(value)

  // ── PrimaryKey ─────────────────────────────────────────────────────────────

  def toPrimaryKeyExpr[S](expr: DdbKeyExpr.PrimaryKey[S]): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    toPrimaryKeyExpr(expr, DynamoDBCodecDeriverConfigure.default[S], Map.empty)

  def toPrimaryKeyExpr[S](
    expr: DdbKeyExpr.PrimaryKey[S],
    config: DynamoDBCodecDeriverConfigure[S],
    fieldNames: Map[String, String]
  ): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    expr match {
      case DdbKeyExpr.PartitionKeyEquals(optic, value, schema) =>
        fieldName(optic, fieldNames).map { name =>
          KeyConditionExpr.PartitionKeyEquals[S](PartitionKey[S, Any](name), encode(value, schema, config))
        }
      case DdbKeyExpr.Composite(pkExpr, skEq)                  =>
        for {
          pkName <- fieldName(pkExpr.optic, fieldNames)
          skName <- fieldName(skEq.optic, fieldNames)
        } yield {
          val pkNode = KeyConditionExpr.PartitionKeyEquals[S](
            PartitionKey[S, Any](pkName),
            encode(pkExpr.value, pkExpr.schema, config)
          )
          val skNode =
            KeyConditionExpr.SortKeyEquals[S](SortKey[S, Any](skName), encode(skEq.value, skEq.schema, config))
          KeyConditionExpr.CompositePrimaryKeyExpr[S](pkNode, skNode)
        }
    }

  // ── Full DdbKeyExpr (adds Extended) ────────────────────────────────────────

  def toKeyConditionExpr[S](expr: DdbKeyExpr[S]): Either[String, KeyConditionExpr[S]] =
    toKeyConditionExpr(expr, DynamoDBCodecDeriverConfigure.default[S], Map.empty)

  def toKeyConditionExpr[S](
    expr: DdbKeyExpr[S],
    config: DynamoDBCodecDeriverConfigure[S],
    fieldNames: Map[String, String]
  ): Either[String, KeyConditionExpr[S]] =
    expr match {
      case pk: DdbKeyExpr.PrimaryKey[S]       =>
        toPrimaryKeyExpr(pk, config, fieldNames)
      case DdbKeyExpr.Extended(pkExpr, skExt) =>
        for {
          pkName <- fieldName(pkExpr.optic, fieldNames)
          skNode <- toExtendedSortKey[S](skExt, config, fieldNames)
        } yield KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[S](
          KeyConditionExpr.PartitionKeyEquals[S](
            PartitionKey[S, Any](pkName),
            encode(pkExpr.value, pkExpr.schema, config)
          ),
          skNode
        )
    }

  // ── Helpers ───────────────────────────────────────────────────────────────

  // Only top-level field optics are valid as DynamoDB key fields; map the raw Scala name
  // to the wire name the calling table's codec configuration produced.
  private def fieldName[S, A](optic: Optic[S, A], fieldNames: Map[String, String]): Either[String, String] =
    OpticToPE.pe(optic) match {
      case Right(ProjectionExpression.MapElement(ProjectionExpression.Root, key)) =>
        Right(fieldNames.getOrElse(key, key))
      case Right(pe)                                                              =>
        Left(s"key field must be a single top-level field, got path: $pe")
      case Left(error)                                                            =>
        Left(error)
    }

  private def toExtendedSortKey[S](
    sortKey: DdbKeyExpr.SortKeyExtended[S],
    config: DynamoDBCodecDeriverConfigure[S],
    fieldNames: Map[String, String]
  ): Either[String, KeyConditionExpr.ExtendedSortKeyExpr[S, _]] =
    sortKey match {
      case DdbKeyExpr.SortKeyExtended.Gt(optic, value, schema)       =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThan(SortKey[S, Any](n), encode(value, schema, config))
        )
      case DdbKeyExpr.SortKeyExtended.Gte(optic, value, schema)      =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.GreaterThanOrEqual(SortKey[S, Any](n), encode(value, schema, config))
        )
      case DdbKeyExpr.SortKeyExtended.Lt(optic, value, schema)       =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThan(SortKey[S, Any](n), encode(value, schema, config))
        )
      case DdbKeyExpr.SortKeyExtended.Lte(optic, value, schema)      =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.LessThanOrEqual(SortKey[S, Any](n), encode(value, schema, config))
        )
      case DdbKeyExpr.SortKeyExtended.Between(optic, lo, hi, schema) =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr
            .Between(SortKey[S, Any](n), encode(lo, schema, config), encode(hi, schema, config))
        )
      case DdbKeyExpr.SortKeyExtended.BeginsWith(optic, prefix)      =>
        fieldName(optic, fieldNames).map(n =>
          KeyConditionExpr.ExtendedSortKeyExpr.BeginsWith(SortKey[S, Any](n), AttributeValue.String(prefix))
        )
    }
}
