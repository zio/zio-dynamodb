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

object DdbGrammar {
  import zio.blocks.schema.comptime.Allows
  import Allows._
  import zio.dynamodb.compat.||

  // scalars
  type N    =
    Primitive.Int || Primitive.Long || Primitive.Float || Primitive.Double || Primitive.Short || Wrapped[Self]
  type S    = Primitive.String || Wrapped[Self]
  type BOOL = Primitive.Boolean
  type B    = Sequence[Primitive.Byte] || Wrapped[Self]
  // I think we can ignore NULL for incoming Scala types

  type NS = Sequence.Set[N || Wrapped[N]]
  type SS = Sequence.Set[S || Wrapped[S]]
  type BS = Sequence.Set[B]

  // list excludes Sets - note we need to explicitly add Record here for List[Address]
  type L = Sequence.List[All || Record[All]] || Sequence.Vector[All || Record[All]] || Sequence.Array[
    All || Record[All]
  ] ||
    Sequence.Chunk[All | Record[All]]

  type M = Map[Primitive.String, All]

  // single recursive root
  type All =
    N || S || BOOL || B || NS || SS || BS || Record[Self] || Sequence[Self] || Map[Self, Self]

}
