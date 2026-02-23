package zio.dynamodb.blocks

import zio.blocks.schema.SchemaExpr.RelationalOperator
import zio.blocks.schema._
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.KeyConditionExpr.{ CompositePrimaryKeyExpr, PartitionKeyEquals }
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.UpdateExpression.SetOperand.PathOperand
import zio.dynamodb._
import zio.stream.Stream

import scala.language.implicitConversions

/*
FUNCTIONALITY MATRIX

Expression Type            | SCHEMA1            | SCHEMA2 |
---------------------------|--------------------|-------|
[X] ProjectionExpression       | Schema1 accessors  | Raw Optic + implicit def -> PE |
[X] Filter/ConditionExpression | ZDDB API           | SchemaExpr + implicit def -> CE  |
[X] UpdateExpression           | ZDDB API           | SchemaExpr + implicit def -> UE |
[X] Primary Keys               | ZDDB API           | SchemaExpr + implicit def -> PKExpr |
[X] QueryAPI                   | single API         | separate entry point, same API  |

implicit conversions
--------------------
Lens[S, A] -> ProjectionExpression[S, A]
Optional[S, A] -> ProjectionExpression[S, A]
optic: Optic[From, To] -> UpdateExpression // via extension methods
 */
object BlocksApi extends Conversions {

  def put[A: SchemaCodec](tableName: String, a: A): DynamoDBQuery[A, Option[A]] =
    DynamoDBQuery
      .putItem(tableName, DynamoDBQuery.toItem(a))
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))

  def get[A, From: SchemaCodec](tableName: String)(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[From, Either[ItemError, From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery.get(tableName, pkExpr.asAttrMap, SchemaCodec[From].projectionsFromSchema)
  }
  def getItem(
    tableName: String,
    key: PrimaryKey,
    projections: ProjectionExpression[_, _]*
  ): DynamoDBQuery[Any, Option[Item]]                                           = DynamoDBQuery.getItem(tableName, key, projections: _*)

  def update[A, From: SchemaCodec](tableName: String)(primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A])(
    action: Action[From]
  ): DynamoDBQuery[From, Option[From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery
      .updateItem(tableName, pkExpr.asAttrMap)(action)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  def deleteFrom[A, From: SchemaCodec](
    tableName: String
  )(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[Any, Option[From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery
      .deleteItem(tableName, pkExpr.asAttrMap)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  /**
   * when executed will return a ZStream of A
   */
  def queryAll[A: SchemaCodec](
    tableName: String
  ): DynamoDBQuery[A, Stream[Throwable, A]] = DynamoDBQuery.queryAll(tableName)

}

trait Conversions {
  implicit def fromLensToProjectionExpression[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(lens)

  implicit def fromOptionalToProjectionExpression[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(optional)

  implicit class OpticToDdbExpr[From, To: ToAttributeValue](optic: Optic[From, To]) {
    // TODO: other ops like ADD etc etc
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(optic),
        UpdateExpression.SetOperand.ValueOperand(ToAttributeValue[To].toAttributeValue(a))
      )
    def set(expr: Optic[From, To]): UpdateExpression.Action.SetAction[From, To] = {
      val self = OpticToPE.pe(optic)
      val pe   = OpticToPE.pe(expr)
      UpdateExpression.Action.SetAction(self, PathOperand(pe))
    }

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

    def between(minValue: To, maxValue: To): ConditionExpression[From] = {
      val pe: ProjectionExpression[From, To] = OpticToPE.pe(optic)
      ConditionExpression.Operand
        .ProjectionExpressionOperand(pe)
        .between(
          ToAttributeValue[To].toAttributeValue(minValue),
          ToAttributeValue[To].toAttributeValue(maxValue)
        )
    }

  }

  implicit def fromSchemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] =
    schemaExprToConditionExpression(expr)

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

  def schemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] = {

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
        val enc                     = schema.derive(DynamoDBCodecDeriver).encoder
        val attrVal: AttributeValue = enc(a)

        val pe           = OpticToPE.pe(o)
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
