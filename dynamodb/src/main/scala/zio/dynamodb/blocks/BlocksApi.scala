package zio.dynamodb.blocks

import zio.blocks.schema.SchemaExpr.RelationalOperator
import zio.dynamodb._
import zio.blocks.schema._
import zio.dynamodb.KeyConditionExpr.PartitionKeyEquals

import scala.language.implicitConversions
import zio.dynamodb.blocks.BlocksCodecViaDynamic.Person.schema

/*
FUNCTIONALITY MATRIX

Expression Type            | ZDDB1              | ZDDB2 |
---------------------------|--------------------|-------|
ProjectionExpression       | Schema1 accessors  | Raw Optic + implicit def -> PE |
Filter/ConditionExpression | ZDDB API           | SchemaExpr + implicit def -> CE  |
UpdateExpression           | ZDDB API           | SchemaExpr + implicit def -> UE |
Primary Keys               | ZDDB API           | SchemaExpr + implicit def -> PKExpr |
QueryAPI                   | single API         | ???                               |

TODO
- Create DynamoDBQuery2
- keep tabs on Schema2 WrapperTypes + Transformations
 */
object BlocksApi {
  implicit def fromLensToProjectionExpression[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(lens)

  implicit def fromOptionalToProjectionExpression[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] =
    OpticToPE.pe(optional)

  implicit def fromSchemaExprToPKExpression[A, B](
    expr: SchemaExpr[A, B]
  ): KeyConditionExpr.PrimaryKeyExpr[A] =
    schemaExprToPartitionKeyExpr(expr)

  implicit def fromSchemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] =
    schemaExprToConditionExpression(expr)

  implicit class OptionalToProjectionExpression[From, To: ToAttributeValue](optional: Optional[From, To]) {
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(optional),
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )
  }

  /*
   Sort and Partition Keys have to be top level scalar values
   */
  implicit class LensToProjectionExpression[From, To: ToAttributeValue](lens: Lens[From, To]) {
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        OpticToPE.pe(lens),
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

  }

  /*
  TODO
  - we could fix A to String/Option[String]
  - explore this Area some more
  - List of DynamoDB specific methods
  - The space of SchemaExpr is larger than DDB - what is exprn is not supported by DDB?
   */

  // TODO: this seems wrong
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

  def schemaExprToPartitionKeyExpr[S, A](
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
      // TODO: Composite use case
      case SchemaExpr.Relational(
            SchemaExpr.Optic(lens: Lens[_, _]),
            SchemaExpr.Literal(a, schema),
            RelationalOperator.Equal
          ) =>
        // get field name from the lens
        topLevelLensFieldName(lens) match {
          case Some(field) =>
            val enc: Encoder[Any]       = BlocksCodec.encoder(schema)
            val attrVal: AttributeValue = enc(a)
            PartitionKeyEquals[S](PartitionKey(field), attrVal)
          case _           =>
            throw new Exception(s"Expected a top level field in the lens, got: $lens")
        }
      case expr =>
        throw new Exception(s"unexpected SchemaExpr: $expr")
    }
  }

  def schemaExprToConditionExpression[A, B](
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

    expr match {
      case SchemaExpr.Relational(SchemaExpr.Optic(o), SchemaExpr.Literal(a, schema), operator) =>
        val pe                      = opticToPE(o)
        val enc                     = BlocksCodec.encoder(schema)
        val attrVal: AttributeValue = enc(a)
        val peOperand               = ConditionExpression.Operand.ProjectionExpressionOperand[A](pe)
        val valueOperand            = ConditionExpression.Operand.ValueOperand[A](attrVal)
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
  object Operand {

    private[dynamodb] final case class ProjectionExpressionOperand[From](pe: ProjectionExpression[From, _])
        extends Operand[From, Any] // TODO: Avi is Any OK?
    private[dynamodb] final case class ValueOperand[From](value: AttributeValue)
        extends Operand[From, Any] // TODO: Avi is Any OK?
    // needs to extend Operand[From, Long]
    private[dynamodb] final case class Size[-From, To](path: ProjectionExpression[From, To], ev: Sizable[To])
        extends Operand[From, Long]

  }

 */

/*
private[dynamodb] final case class Equals[From](left: Operand[From, Any], right: Operand[From, Any])
    extends ConditionExpression[Any]
private[dynamodb] final case class And[From](left: ConditionExpression[From], right: ConditionExpression[From])
    extends ConditionExpression[From]
private[dynamodb] final case class GreaterThan[From](left: Operand[From, Any], right: Operand[From, Any])
    extends ConditionExpression[Any]
private[dynamodb] final case class Contains[From](path: ProjectionExpression[From, _], value: AttributeValue)
    extends ConditionExpression[From]


ConditionExpression
AttributeExists in ConditionExpression$ (zio.dynamodb)
NotEqual in ConditionExpression$ (zio.dynamodb)
AttributeType in ConditionExpression$ (zio.dynamodb)
And in ConditionExpression$ (zio.dynamodb)
LessThanOrEqual in ConditionExpression$ (zio.dynamodb)
Contains in ConditionExpression$ (zio.dynamodb)
Not in ConditionExpression$ (zio.dynamodb)
Between in ConditionExpression$ (zio.dynamodb)
LessThan in ConditionExpression$ (zio.dynamodb)
Equals in ConditionExpression$ (zio.dynamodb)
AttributeNotExists in ConditionExpression$ (zio.dynamodb)
In in ConditionExpression$ (zio.dynamodb)
Or in ConditionExpression$ (zio.dynamodb)
GreaterThan in ConditionExpression$ (zio.dynamodb)
BeginsWith in ConditionExpression$ (zio.dynamodb)
GreaterThanOrEqual in ConditionExpression$ (zio.dynamodb)

Operand
Size in Operand$ in ConditionExpression$ (zio.dynamodb)
ValueOperand in Operand$ in ConditionExpression$ (zio.dynamodb)
ProjectionExpressionOperand in Operand$ in ConditionExpression$ (zio.dynamodb)


 */
