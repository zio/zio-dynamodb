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

package zio.dynamodb.blocks

import zio.blocks.schema.{ DynamicOptic, DynamicValue, Lens, Optic, Optional, PrimitiveValue }
import zio.dynamodb.ProjectionExpression

object OpticToPE {

  def pe(dynamicOptic: DynamicOptic): Either[String, ProjectionExpression[_, _]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodes                              = dynamicOptic.nodes
    var idx                                = 0
    var error: String                      = null
    while (idx < nodes.length && error == null) {
      nodes(idx) match {
        case DynamicOptic.Node.Field(name) => prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                          => error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe)
  }

  def pe[S, A](optic: Optic[S, A]): Either[String, ProjectionExpression[S, A]] =
    optic match {
      case lens: Lens[S, A]         => pe(lens)
      case optional: Optional[S, A] => pe(optional)
      case _                        => Left(s"unexpected optic: $optic")
    }

  // Lens[S, A] implies no indexed access anywhere in the path — MapElements all the way down.
  def pe[S, A](lens: Lens[S, A]): Either[String, ProjectionExpression[S, A]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodes                              = lens.toDynamic.nodes
    var idx                                = 0
    var error: String                      = null
    while (idx < nodes.length && error == null) {
      nodes(idx) match {
        case DynamicOptic.Node.Field(name) => prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                          => error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe.asInstanceOf[ProjectionExpression[S, A]])
  }

  private[this] final def pruneOptionalNodes(nodes: IndexedSeq[DynamicOptic.Node]): IndexedSeq[DynamicOptic.Node] = {
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
  def pe[S, A](optional: Optional[S, A]): Either[String, ProjectionExpression[S, A]] = {
    var prevPe: ProjectionExpression[_, _] = ProjectionExpression.Root
    val nodesPruned                        = pruneOptionalNodes(optional.toDynamic.nodes)
    var idx                                = 0
    var error: String                      = null
    while (idx < nodesPruned.length && error == null) {
      nodesPruned(idx) match {
        case DynamicOptic.Node.Field(name)                                                  =>
          prevPe = ProjectionExpression.MapElement(prevPe, name)
        case DynamicOptic.Node.AtIndex(index)                                               =>
          prevPe = ProjectionExpression.ListElement(prevPe, index)
        case DynamicOptic.Node.AtMapKey(DynamicValue.Primitive(PrimitiveValue.String(key))) =>
          prevPe = ProjectionExpression.MapElement(prevPe, key)
        case DynamicOptic.Node.AtMapKey(key)                                                =>
          error = s"found map key '$key' — only String keys are supported in DDB"
        case DynamicOptic.Node.Case(name)                                                   =>
          // Key (tagged) encoding (the default): the case name is a map key in DDB.
          // e.g. Shape.Circle encodes as { "Circle": { "radius": 1.5 } }, so
          // Case("Circle") must become MapElement(prev, "Circle") in the path.
          // Only correct for DiscriminatorKind.Key; Field/None encoding users must not
          // use optics through variant cases.
          prevPe = ProjectionExpression.MapElement(prevPe, name)
        case node                                                                           =>
          error = s"unexpected node: $node"
      }
      idx += 1
    }
    if (error != null) Left(error) else Right(prevPe.asInstanceOf[ProjectionExpression[S, A]])
  }

}
