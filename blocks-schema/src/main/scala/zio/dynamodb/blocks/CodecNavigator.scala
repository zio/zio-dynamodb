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

import zio.blocks.schema.DynamicOptic
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.blocks.schema.DynamoDBCodec

/**
 * Resolves an optic path against the codec that writes the item: the DynamoDB attribute path and the
 * codec at that position. Names, discriminators and per-position overrides therefore always match what
 * the codec encodes.
 */
private[dynamodb] object CodecNavigator {

  final class Resolved(val path: ProjectionExpression[_, _], val codec: DynamoDBCodec[Any])

  def resolve(codec: DynamoDBCodec[_], optic: DynamicOptic): Either[String, Resolved] = {
    var path: ProjectionExpression[_, _] = ProjectionExpression.Root
    var current: DynamoDBCodec[Any]      = codec.asInstanceOf[DynamoDBCodec[Any]]
    val nodes                            = optic.nodes
    var error: String                    = null
    var idx                              = 0
    while (idx < nodes.length && (error eq null)) {
      val node = nodes(idx)
      // A traversal reaches every element, value or key at once, which no single attribute path names.
      if (
        (node eq DynamicOptic.Node.Elements) || (node eq DynamicOptic.Node.MapValues) ||
        (node eq DynamicOptic.Node.MapKeys)
      ) error = s"'$path' is followed by a traversal ($node), which has no single DynamoDB attribute path"
      else
        current.codecAt(node) match {
          case Right(atSegment) =>
            atSegment.segment match {
              case DynamoDBCodec.Segment.Name(name)   => path = ProjectionExpression.MapElement(path, name)
              case DynamoDBCodec.Segment.Index(index) => path = ProjectionExpression.ListElement(path, index)
              case DynamoDBCodec.Segment.Transparent  =>
            }
            current = atSegment.codec
          case Left(msg)        => error = msg
        }
      idx += 1
    }
    if (error ne null) Left(error) else Right(new Resolved(path, current))
  }
}
