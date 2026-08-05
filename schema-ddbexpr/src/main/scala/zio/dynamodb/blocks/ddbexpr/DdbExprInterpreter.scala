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
import zio.blocks.schema.{ DynamicSchemaExpr, Schema }
import zio.dynamodb.blocks.OpticToPE
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }
import zio.dynamodb.{ AttributeValue, ConditionExpression, ProjectionExpression }

/**
 * Interprets a [[DdbExpr]][S, Boolean] into a [[ConditionExpression]][S].
 *
 *  DDB function nodes ([[DdbExpr.Between]], [[DdbExpr.In]], etc.) carry a
 *  [[zio.dynamodb.blocks.schema.DynamoDBCodec]] and encode literals directly.
 *
 *  [[DdbExpr.Builtin]] wraps a [[zio.blocks.schema.SchemaExpr]] for scalar
 *  comparisons (===, >, <, etc.). Since zio-blocks v0.0.47 [[zio.blocks.schema.DynamicSchemaExpr.Literal]]
 *  carries a [[Schema]], [[fromDynamicSchemaExpr]] derives a [[zio.dynamodb.blocks.schema.DynamoDBCodec]]
 *  from it and encodes via the codec — so sealed-trait `enumValuesAsStrings` rules are
 *  preserved without any special workaround at the call site.
 */
object DdbExprInterpreter {

  // Keyed by Schema reference identity — same rationale as DdbExprApi.CodecCacheKey:
  // companion-object vals are class-loader-scoped singletons so reference identity is
  // both safe and sufficient; structural equality risks cross-classloader collisions.
  private final class SchemaKey(val s: AnyRef) {
    override val hashCode: Int           = System.identityHashCode(s)
    override def equals(o: Any): Boolean = o match {
      case k: SchemaKey => s eq k.s
      case _            => false
    }
  }
  private val codecCache = new ConcurrentHashMap[SchemaKey, DynamoDBCodec[Any]]()

  def toConditionExpression[S](expr: DdbExpr[S, Boolean]): Either[String, ConditionExpression[S]] =
    expr match {

      case DdbExpr.Builtin(se) =>
        Right(fromDynamicSchemaExpr[S](se.dynamic))

      case DdbExpr.And(l, r) =>
        for { lce <- toConditionExpression(l); rce <- toConditionExpression(r) } yield ConditionExpression.And(lce, rce)

      case DdbExpr.Or(l, r) =>
        for { lce <- toConditionExpression(l); rce <- toConditionExpression(r) } yield ConditionExpression.Or(lce, rce)

      case DdbExpr.Not(inner) =>
        toConditionExpression(inner).map(ConditionExpression.Not(_))

      case DdbExpr.AttributeExists(optic) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.AttributeExists(pe.asInstanceOf[ProjectionExpression[S, Any]])
        }

      case DdbExpr.AttributeNotExists(optic) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.AttributeNotExists(pe.asInstanceOf[ProjectionExpression[S, Any]])
        }

      case DdbExpr.Between(optic, lo, hi, codec) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.Between(
            ConditionExpression.Operand.ProjectionExpressionOperand(pe.asInstanceOf[ProjectionExpression[S, Any]]),
            codec.encoder(lo),
            codec.encoder(hi)
          )
        }

      case DdbExpr.In(optic, values, codec) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.In(
            ConditionExpression.Operand.ProjectionExpressionOperand(pe.asInstanceOf[ProjectionExpression[S, Any]]),
            values.map(codec.encoder).toSet
          )
        }

      case DdbExpr.BeginsWith(optic, prefix) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.BeginsWith(
            pe.asInstanceOf[ProjectionExpression[S, Any]],
            AttributeValue.String(prefix)
          )
        }

      case DdbExpr.Contains(optic, value) =>
        OpticToPE.pe(optic).map { pe =>
          ConditionExpression.Contains(
            pe.asInstanceOf[ProjectionExpression[S, Any]],
            AttributeValue.String(value)
          )
        }

      case elem: DdbExpr.ContainsElement[S, _, _] =>
        OpticToPE.pe(elem.optic).map { pe =>
          ConditionExpression.Contains(
            pe.asInstanceOf[ProjectionExpression[S, Any]],
            elem.elemCodec.encoder(elem.element)
          )
        }
    }

  // Folds a DynamicSchemaExpr (from zio-blocks-schema) into a ConditionExpression directly;
  // DynamoDBCodecDeriver.dynamicValueCodec (from schema-dynamodb) is available for encoding.
  private def fromDynamicSchemaExpr[A](dse: DynamicSchemaExpr): ConditionExpression[A] = {

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
        OpticToPE.pe(path) match {
          case Left(msg) => ConditionExpression.Failure(msg)
          case Right(pe) =>
            val peOpd  = ConditionExpression.Operand
              .ProjectionExpressionOperand[A](pe.asInstanceOf[ProjectionExpression[A, Any]])
            val valOpd = ConditionExpression.Operand
              .ValueOperand[A](encodeFromSchema(dynValue, schema))
            toRelational(peOpd, valOpd, op)
        }

      case DynamicSchemaExpr.Relational(DynamicSchemaExpr.Select(l), DynamicSchemaExpr.Select(r), op) =>
        (OpticToPE.pe(l), OpticToPE.pe(r)) match {
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
        val leftCE  = fromDynamicSchemaExpr[A](left)
        val rightCE = fromDynamicSchemaExpr[A](right)
        op match {
          case DynamicSchemaExpr.LogicalOperator.And => ConditionExpression.And(leftCE, rightCE)
          case DynamicSchemaExpr.LogicalOperator.Or  => ConditionExpression.Or(leftCE, rightCE)
        }

      case DynamicSchemaExpr.Not(inner) =>
        ConditionExpression.Not(fromDynamicSchemaExpr[A](inner))

      case other =>
        ConditionExpression.Failure(s"unexpected DynamicSchemaExpr: $other")
    }
  }

  private def encodeFromSchema(dv: zio.blocks.schema.DynamicValue, schema: Schema[_]): AttributeValue = {
    val s = schema.asInstanceOf[Schema[Any]]
    s.fromDynamicValue(dv)
      .fold(
        _ => DynamoDBCodecDeriver.dynamicValueCodec.encoder(dv),
        a => {
          val codec = codecCache.computeIfAbsent(new SchemaKey(schema), _ => s.deriving(DynamoDBCodecDeriver).derive)
          codec.encoder(a)
        }
      )
  }
}
