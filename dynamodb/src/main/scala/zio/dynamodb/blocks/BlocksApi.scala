package zio.dynamodb.blocks

import zio.blocks.schema.SchemaExpr.RelationalOperator
import zio.blocks.schema._
import zio.dynamodb.KeyConditionExpr.{ CompositePrimaryKeyExpr, PartitionKeyEquals }
import zio.dynamodb._

import scala.language.implicitConversions

/*
FUNCTIONALITY MATRIX

Expression Type            | SCHEMA1            | SCHEMA2 |
---------------------------|--------------------|-------|
[X] ProjectionExpression       | Schema1 accessors  | Raw Optic + implicit def -> PE |
[X] Filter/ConditionExpression | ZDDB API           | SchemaExpr + implicit def -> CE  |
[X] UpdateExpression           | ZDDB API           | SchemaExpr + implicit def -> UE |
[X] Primary Keys               | ZDDB API           | SchemaExpr + implicit def -> PKExpr |
[X] QueryAPI                   | single API         | single API                               |

 */
object BlocksApi extends LowPrioritySchemaExprConversions {

  implicit def fromLensToProjectionExpression[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(lens)

  implicit def fromOptionalToProjectionExpression[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(optional)

  implicit class OpticToUpdateExpression[From, To: ToAttributeValue](optic: Optic[From, To]) {
    // TODO: other ops like ADD etc etc
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(optic),
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )

    def add(a: To): UpdateExpression.Action.AddAction[From] =
      UpdateExpression.Action.AddAction(
        OpticToPE.pe(optic),
        ToAttributeValue[To].toAttributeValue(a)
      )

    def remove: UpdateExpression.Action.RemoveAction[From] =
      UpdateExpression.Action.RemoveAction(
        OpticToPE.pe(optic)
      )

    def setIfNotExists(a: To): UpdateExpression.Action.SetAction[From, To] = {
      val pe = OpticToPE.pe(optic)
      UpdateExpression.Action.SetAction(
        pe,
        UpdateExpression.SetOperand.IfNotExists(
          pe,
          ToAttributeValue[To].toAttributeValue(a)
        )
      )
    }
  }

  def schemaExprToPrimaryKeyExprUnsafe[S, A](
    expr: SchemaExpr[S, A]
  ): KeyConditionExpr.PrimaryKeyExpr[S] =
    schemaExprToPrimaryKeyExpr(expr) match {
      case Right(pkExpr) => pkExpr
      case Left(error)   => throw new IllegalArgumentException(s"Failed to convert SchemaExpr to PrimaryKeyExpr: $error")
    }

  def schemaExprToKeyConditionExprUnsafe[S, A](
    expr: SchemaExpr[S, A]
  ): KeyConditionExpr[S] =
    schemaExprToPrimaryKeyExpr(expr) match {
      case Right(pkExpr) => pkExpr
      case Left(_)       =>
        toKeyConditionExpr(expr) match {
          case Right(extended) => extended
          case Left(error)     =>
            throw new IllegalArgumentException(s"Failed to convert SchemaExpr $expr to a KeyConditionExpr: $error")
        }
    }

  private[blocks] def schemaExprToPrimaryKeyExpr[S, A](
    expr: SchemaExpr[S, A]
  ): Either[String, KeyConditionExpr.PrimaryKeyExpr[S]] =
    expr match {
      // simplest use case - a single partition key at the top level with an equality op to a literal value
      case SchemaExpr.Relational(
            SchemaExpr.Optic(lens: Lens[_, _]),
            SchemaExpr.Literal(a, schema),
            RelationalOperator.Equal
          ) =>
        // get field name from the lens
        val field                   = topLevelLensFieldNameUnsafe(lens)
        val enc                     = schema.derive(DynamoDBCodecDeriver).encoder
        val attrVal: AttributeValue = enc(a)
        Right(PartitionKeyEquals[S](PartitionKey(field), attrVal))
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
        val pkEquals = {
          val field                   = topLevelLensFieldNameUnsafe(pkLens)
          val enc                     = pkSchema.derive(DynamoDBCodecDeriver).encoder
          val attrVal: AttributeValue = enc(pkVal)
          PartitionKeyEquals[S](PartitionKey(field), attrVal)
        }
        val skEquals: KeyConditionExpr.SortKeyEquals[S] = {
          val field                   = topLevelLensFieldNameUnsafe(skLens)
          val enc                     = skSchema.derive(DynamoDBCodecDeriver).encoder
          val attrVal: AttributeValue = enc(skVal)
          KeyConditionExpr.SortKeyEquals[S](SortKey(field), attrVal)
        }
        Right(CompositePrimaryKeyExpr(pkEquals, skEquals))
      case expr =>
        Left(s"unexpected SchemaExpr: $expr")
    }

  private def topLevelLensFieldNameUnsafe[S, A](lens: Lens[S, A]): String = {
    val nodes = lens.toDynamic.nodes
    if (nodes.length != 1)
      throw new Exception(s"Expected a single node in the lens, got: ${nodes.length}")
    else
      nodes(0) match {
        case DynamicOptic.Node.Field(name) =>
          name
        case _                             => throw new Exception(s"Expected a field node in the lens, got: ${nodes(0)}")
      }

  }

  private[blocks] def toKeyConditionExpr[S, A](
    expr: SchemaExpr[S, A]
  ): Either[String, KeyConditionExpr.ExtendedCompositePrimaryKeyExpr[S]] =
    expr match {
      case SchemaExpr.Logical(
            SchemaExpr.Relational(
              SchemaExpr.Optic(pkLens: Lens[_, _]),
              SchemaExpr.Literal(pkVal, pkSchema),
              RelationalOperator.Equal
            ),
            SchemaExpr.Relational(
              SchemaExpr.Optic(skLens: Lens[_, _]),
              SchemaExpr.Literal(skVal, skSchema),
              nonEqualityOp
            ),
            SchemaExpr.LogicalOperator.And
          ) =>
        val pkEquals = {
          val field                   = topLevelLensFieldNameUnsafe(pkLens)
          val enc                     = pkSchema.derive(DynamoDBCodecDeriver).encoder
          val attrVal: AttributeValue = enc(pkVal)
          PartitionKeyEquals[S](PartitionKey(field), attrVal)
        }
        val skCompare: KeyConditionExpr.ExtendedSortKeyExpr[S, A] = {
          val field                   = topLevelLensFieldNameUnsafe(skLens)
          val enc                     = skSchema.derive(DynamoDBCodecDeriver).encoder
          val attrVal: AttributeValue = enc(skVal)
          println(s"XXXXXXXXX $nonEqualityOp")
          nonEqualityOp match {
            case RelationalOperator.GreaterThan =>
              KeyConditionExpr.ExtendedSortKeyExpr.GreaterThan(SortKey(field), attrVal)
            case _                              => ??? // TODO: Avi - implement other ops like LessThan, GreaterThan, LessThanOrEqual
          }
        }
        Right(KeyConditionExpr.ExtendedCompositePrimaryKeyExpr(pkEquals, skCompare))
      case expr =>
        Left(s"unexpected SchemaExpr for ExtendedCompositePrimaryKeyExpr: $expr")
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
