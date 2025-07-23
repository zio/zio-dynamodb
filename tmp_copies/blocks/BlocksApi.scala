package zio.dynamodb.blocks

import zio.dynamodb._
import zio.blocks.schema._

import scala.language.implicitConversions
import zio.dynamodb.blocks.BlocksCodecViaDynamic.Person.schema
import zio.dynamodb.proofs.IsPrimaryKey

/*
low level API vs new SchemaExpr API?- we now have Person.name > ""
do we keep the 1 API + Phantom Type approach?
does Blocks macro honour implicit schema transformations in scope?
 */
object BlocksApi {
  implicit def fromLensToProjectionExpression[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
    ToPE.pe(lens)

  implicit def fromOptionalToProjectionExpression[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] =
    ToPE.pe(optional)

  implicit def fromSchemaExprToPKExpression[A, B](
    expr: SchemaExpr[A, B]
  ): KeyConditionExpr.PrimaryKeyExpr[A] =
    schemaExprToPKExpression(expr)

  implicit def fromSchemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] =
    schemaExprToConditionExpression(expr)

  def opticToPE[S, A](optic: Optic[S, A]): ProjectionExpression[S, A] =
    optic match {
      case l: Lens[S, A]     =>
        ToPE.pe(l)
      case o: Optional[S, A] =>
        ToPE.pe(o)
      case _                 =>
        throw new Exception("not a lens")
    }

  object ToPE {
    def pe[S, A](lens: Lens[S, A]): ProjectionExpression[S, A] =
      lens.source match {
        case r @ Reflect.Record(fields, _, _, _, _) =>
          var idx                            = 0
          var maybeFieldName: Option[String] = None
          while (idx < fields.length && maybeFieldName.isEmpty) {
            val field = fields(idx)
            if (r.lensByName(field.name).contains(lens))
              maybeFieldName = new Some(field.name)
            idx += 1
          }
          maybeFieldName match {
            case Some(fieldName) =>
              ProjectionExpression.MapElement[S, A](ProjectionExpression.Root, fieldName)
            case None            =>
              throw new Exception("could not find field name for lens")
          }
        case _                                      =>
          throw new Exception("not a schema")
      }

    /*
    object Node {
      case class Field(name: String) extends Node
      case class Case(name: String) extends Node
      case class AtIndex(index: Int) extends Node
      case class AtMapKey[K](key: K) extends Node
      case class AtIndices(index: Seq[Int]) extends Node
      case class AtMapKeys[K](keys: Seq[K]) extends Node
      case object Elements extends Node
      case object MapKeys extends Node
      case object MapValues extends Node
    }
     */

    /*
    nodes: Vector(Field(maybeMap), Case(Some), Field(value), AtMapKey(a))
    TODO: how did we handle SUM TYPES? I think they were just MapElements
    while index is not last
    - if current node is Case(Some) and next node is Field(value)

val result = {
  val b = Vector.newBuilder[Node]
  var i = 0
  while (i < vec.length) {
    if (
      i + 1 < vec.length &&
      vec(i) == Case("Some") &&
      vec(i + 1) == Field("value")
    ) {
      i += 2 // skip both
    } else {
      b += vec(i)
      i += 1
    }
  }
  b.result()
}

     */
    def pruneOptionalNodes(nodes: IndexedSeq[DynamicOptic.Node]): IndexedSeq[DynamicOptic.Node] = {
      val builder = Vector.newBuilder[DynamicOptic.Node]
      var i       = 0
      while (i < nodes.length)
        if (
          i + 1 < nodes.length &&
          nodes(i) == DynamicOptic.Node.Case("Some") &&
          nodes(i + 1) == DynamicOptic.Node.Field("value")
        )
          i += 2 // skip both
        else {
          builder += nodes(i)
          i += 1
        }
      builder.result()
    }

    def pe[S, A](optional: Optional[S, A]): ProjectionExpression[S, A] = {

      var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
      val nodes                              = optional.toDynamic.nodes
      val nodesPruned                        = pruneOptionalNodes(nodes)

      var idx = 0
      while (idx < nodesPruned.length) {
        val node   = nodesPruned(idx)
        val nextPe = node match {
          case DynamicOptic.Node.Field(name)           =>
            ProjectionExpression.MapElement(prevPe, name)
          case DynamicOptic.Node.AtIndex(index)        =>
            ProjectionExpression.ListElement(prevPe, index)
          case DynamicOptic.Node.AtMapKey(key: String) => // Only String Keys are supported in DDB
            ProjectionExpression.MapElement(prevPe, key)
          // TODO: handle all Node types
          case DynamicOptic.Node.AtMapKey(key)         =>
            throw new Exception(s"Only String Keys are supported in DDB")
          case DynamicOptic.Node.Case(_)               => // We only need to deal with non optional SOME TYPES here
            prevPe
          case _                                       => throw new Exception(s"unexpected node: $node")
        }
        prevPe = nextPe
        idx += 1
      }
      prevPe.asInstanceOf[ProjectionExpression[S, A]]
    }

  }

  implicit class OptionalToProjectionExpression[From, To: ToAttributeValue](optional: Optional[From, To]) {
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        ToPE.pe(optional),
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )
  }

  /*
   Sort and Partition Keys have to be top level scalar values
   */
  implicit class LensToProjectionExpression[From, To: ToAttributeValue](lens: Lens[From, To]) {
    def set(a: To): UpdateExpression.Action.SetAction[From, To] =
      UpdateExpression.Action.SetAction(
        ToPE.pe(lens),
        UpdateExpression.SetOperand.ValueOperand(implicitly[ToAttributeValue[To]].toAttributeValue(a))
      )

    def partitionKey(implicit ev: IsPrimaryKey[To]): PartitionKey[From, To] = {
      val _  = ev
      val pe = ToPE.pe(lens)
      pe match {
        case ProjectionExpression.MapElement(_, key) => PartitionKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a partition key") // should not happen
      }
    }
    def sortKey(implicit ev: IsPrimaryKey[To]): SortKey[From, To] = {
      val _  = ev
      val pe = ToPE.pe(lens)
      pe match {
        case ProjectionExpression.MapElement(_, key) => SortKey[From, To](key)
        case _                                       => throw new IllegalArgumentException("Not a sort key") // should not happen
      }
    }

  }

  /*
  TODO
  - we could fix A to String/Option[String]
  - explore this Area some more
  - List of DynamoDB specific methods
  - The space of SchemaExpr is larger than DDB - what is exprn is not supported by DDB?
   */

  /**
   * DynamoDb specific methods are implemented as extension methods
   */
  // implicit class DynamoOpticsSyntax[S, A](optic: Optic[S, A])      {
  //   def beginsWith(value: String)(implicit ev: Beginnable[String, A]): ConditionExpression[S] =
  //     ConditionExpression.BeginsWith(opticToPE(optic), AttributeValue.String(value))
  // }
  implicit class SchemaExprSyntax[A](expr: SchemaExpr[A, Boolean])                           {
    def And(that: SchemaExpr[A, Boolean]): SchemaExpr[A, Boolean] =
      SchemaExpr.Logical[A](expr, that, SchemaExpr.LogicalOperator.And)
    def Or(that: SchemaExpr[A, Boolean]): SchemaExpr[A, Boolean]  =
      SchemaExpr.Logical[A](expr, that, SchemaExpr.LogicalOperator.Or)
  }
  implicit class ConditionExpressionAndSchemaExprSyntax[A](condExpr: ConditionExpression[A]) {
    def And(that: SchemaExpr[A, Boolean]): ConditionExpression[A] =
      condExpr && schemaExprToConditionExpression(that)
    def Or(that: SchemaExpr[A, Boolean]): ConditionExpression[A]  =
      condExpr || schemaExprToConditionExpression(that)
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

  def schemaExprToPKExpression[A, B](
    expr: SchemaExpr[A, B]
  ): KeyConditionExpr.PrimaryKeyExpr[A] = ???

  def schemaExprToConditionExpression[A, B](
    expr: SchemaExpr[A, B]
  ): ConditionExpression[A] =
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
