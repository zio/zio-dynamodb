package zio.dynamodb.blocks

import zio.blocks.schema.{ DynamicOptic, Lens, Optional, Reflect }
import zio.dynamodb.ProjectionExpression

object OpticToPE {
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
