package zio.dynamodb.blocks

import zio.blocks.schema.SchemaExpr.RelationalOperator
import zio.dynamodb._
import zio.blocks.schema._
import zio.dynamodb.KeyConditionExpr.{ CompositePrimaryKeyExpr, PartitionKeyEquals }

import scala.language.implicitConversions

/*
FUNCTIONALITY MATRIX

Expression Type            | ZDDB1              | ZDDB2 |
---------------------------|--------------------|-------|
ProjectionExpression       | Schema1 accessors  | Raw Optic + implicit def -> PE |
Filter/ConditionExpression | ZDDB API           | SchemaExpr + implicit def -> CE  |
UpdateExpression           | ZDDB API           | SchemaExpr + implicit def -> UE |
Primary Keys               | ZDDB API           | SchemaExpr + implicit def -> PKExpr |
QueryAPI                   | single API         | single API                               |

TODO
- Schema2 WrapperTypes + Transformations
 */
object BlocksApi {
  implicit class SchemaExprOps[A, B](expr: SchemaExpr[A, B]) {

    final def &&[B2](
      that: SchemaExpr[A, B2]
    )(implicit ev: B <:< Boolean, ev2: B2 =:= Boolean): SchemaExpr[A, Boolean] =
      SchemaExpr.Logical(
        expr.asEquivalent[Boolean],
        that.asEquivalent[Boolean],
        SchemaExpr.LogicalOperator.And
      )

    final def ||[B2](
      that: SchemaExpr[A, B2]
    )(implicit ev: B <:< Boolean, ev2: B2 =:= Boolean): SchemaExpr[A, Boolean] =
      SchemaExpr.Logical(
        expr.asEquivalent[Boolean],
        that.asEquivalent[Boolean],
        SchemaExpr.LogicalOperator.Or
      )

    final def asEquivalent[B2](implicit ev: B <:< B2): SchemaExpr[A, B2] =
      expr.asInstanceOf[SchemaExpr[A, B2]]

  }

  implicit def fromLensToProjectionExpression[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(lens)

  implicit def fromOptionalToProjectionExpression[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(optional)

  implicit def fromSchemaExprToPKExpression[A, B](
    expr: SchemaExpr[A, B]
  ): KeyConditionExpr.PrimaryKeyExpr[A] =
    schemaExprToPrimaryKeyExpr(expr)

  implicit def fromSchemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] =
    schemaExprToConditionExpression(expr)

  implicit class OptionalToUpdateExpression[From, To: ToAttributeValue](optional: Optional[From, To]) {
    // TODO: other ops like ADD etc etc
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(optional),
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )
  }

  implicit class LensToUpdateExpression[From, To: ToAttributeValue](lens: Lens[From, To]) {
    // TODO: other ops like ADD etc etc
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(lens),
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )

  }

  private def schemaExprToPrimaryKeyExpr[S, A](
    expr: SchemaExpr[S, A]
  ): KeyConditionExpr.PrimaryKeyExpr[S] = {
    def topLevelLensFieldName[S, A](lens: Lens[S, A]): Option[String] = {
      val nodes = lens.toDynamic.nodes
      if (nodes.length != 1)
        None
      else
        nodes(0) match {
          case DynamicOptic.Node.Field(name) =>
            Some(name)
          case _                             => None
        }

    }

    expr match {
      // simplest use case - a single partition key at the top level with an equality op to a literal value
      case SchemaExpr.Relational(
            SchemaExpr.Optic(lens: Lens[_, _]),
            SchemaExpr.Literal(a, schema),
            RelationalOperator.Equal
          ) =>
        // get field name from the lens
        topLevelLensFieldName(lens) match {
          case Some(field) =>
            val enc                     = schema.derive(DynamoDBBlocks.Deriver).encoder
            val attrVal: AttributeValue = enc(a)
            PartitionKeyEquals[S](PartitionKey(field), attrVal)
          case _           =>
            throw new Exception(s"Expected a top level field in the lens, got: $lens")
        }
      // composite primary key expression - partition key equality And sort key equality
      case SchemaExpr.Logical(
            SchemaExpr.Relational(
              SchemaExpr.Optic(pkLens: Lens[_, _]),
              SchemaExpr.Literal(pkVal, pkSchema),
              RelationalOperator.Equal
            ),
            SchemaExpr.Relational(
              SchemaExpr.Optic(skLens: Lens[_, _]),
              SchemaExpr.Literal(skVal, skSchema),
              RelationalOperator.Equal // TODO: expand to other ops
            ),
            SchemaExpr.LogicalOperator.And
          ) =>
        val pkEquals = topLevelLensFieldName(pkLens) match {
          case Some(field) =>
            val enc                     = pkSchema.derive(DynamoDBBlocks.Deriver).encoder
            val attrVal: AttributeValue = enc(pkVal)
            PartitionKeyEquals[S](PartitionKey(field), attrVal)
          case _           =>
            throw new Exception(s"Expected a top level field in the lens, got: $pkLens")
        }
        val skEquals = topLevelLensFieldName(skLens) match {
          case Some(field) =>
            val enc                     = skSchema.derive(DynamoDBBlocks.Deriver).encoder
            val attrVal: AttributeValue = enc(skVal)
            KeyConditionExpr.SortKeyEquals[S](SortKey(field), attrVal)
          case _           =>
            throw new Exception(s"Expected a top level field in the lens, got: $skLens")
        }
        CompositePrimaryKeyExpr(pkEquals, skEquals)
      case expr =>
        throw new Exception(s"unexpected SchemaExpr: $expr")
    }
  }

  private def schemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] = {
    def opticToPE[S, A](optic: Optic[S, A]): ProjectionExpression[S, A] =
      optic match {
        case l: Lens[S, A]     =>
          OpticToPE.pe(l)
        case o: Optional[S, A] =>
          OpticToPE.pe(o)
        case _                 =>
          throw new Exception("not a lens")
      }

    def toRelationalConditionExpression[A](
      left: ConditionExpression.Operand[A, _],
      right: ConditionExpression.Operand[A, _],
      operator: SchemaExpr.RelationalOperator
    ): ConditionExpression[A] =
      operator match {
        case SchemaExpr.RelationalOperator.GreaterThanOrEqual =>
          ConditionExpression.GreaterThanOrEqual(left, right)
        case SchemaExpr.RelationalOperator.GreaterThan        =>
          ConditionExpression.GreaterThan(left, right)
        case SchemaExpr.RelationalOperator.LessThanOrEqual    =>
          ConditionExpression.LessThanOrEqual(left, right)
        case SchemaExpr.RelationalOperator.LessThan           =>
          ConditionExpression.LessThan(left, right)
        case SchemaExpr.RelationalOperator.Equal              =>
          ConditionExpression.Equals(left, right)
        case SchemaExpr.RelationalOperator.NotEqual           =>
          ConditionExpression.NotEqual(left, right)
      }

    def toLogicalConditionExpression[A](
      left: ConditionExpression[A],
      right: ConditionExpression[A],
      operator: SchemaExpr.LogicalOperator
    ): ConditionExpression[A] =
      operator match {
        case SchemaExpr.LogicalOperator.And =>
          ConditionExpression.And(left, right)
        case SchemaExpr.LogicalOperator.Or  =>
          ConditionExpression.Or(left, right)
      }

    expr match {
      case SchemaExpr.Relational(SchemaExpr.Optic(o), SchemaExpr.Literal(a, schema), operator) =>
        val enc                     = schema.derive(DynamoDBBlocks.Deriver).encoder
        val attrVal: AttributeValue = enc(a)

        val pe           = opticToPE(o)
        val peOperand    = ConditionExpression.Operand.ProjectionExpressionOperand[A](pe)
        val valueOperand = ConditionExpression.Operand.ValueOperand[A](attrVal)
        toRelationalConditionExpression(peOperand, valueOperand, operator)
      case SchemaExpr.Logical(left, right, logicalOperator)                                    =>
        toLogicalConditionExpression(
          schemaExprToConditionExpression(left),
          schemaExprToConditionExpression(right),
          logicalOperator
        )
      case expr                                                                                =>
        throw new Exception(s"unexpected SchemaExpr: $expr")
    }
  }

}

/*
ConditionExpression
  AttributeExists
  NotEqual
  AttributeType
  And
  LessThanOrEqual
  Contains
  Not
  Between
  LessThan
  Equals
  AttributeNotExists
  In
  Or
  GreaterThan
  BeginsWith
  GreaterThanOrEqual

Operand extends ConditionExpression
  Size
  ValueOperand
  ProjectionExpressionOperand
 */
