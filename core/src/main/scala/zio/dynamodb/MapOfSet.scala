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

package zio.dynamodb

import scala.collection.immutable.{ Map => ScalaMap }

private[dynamodb] final case class MapOfSet[K, V] private (private val map: ScalaMap[K, Set[V]])
    extends Iterable[(K, Set[V])] { self =>

  def getOrElse(key: K, default: => Set[V]): Set[V] = map.getOrElse(key, default)

  def get(key: K): Option[Set[V]] = map.get(key)

  def +(entry: (K, V)): MapOfSet[K, V] = {
    val (key, value) = entry
    val newEntry     = self.map.get(key).fold((key, Set(value)))(set => (key, set + value))
    new MapOfSet(self.map + newEntry)
  }

  def ++(that: MapOfSet[K, V]): MapOfSet[K, V] = {
    val xs: Seq[(K, Set[V])]   = that.map.toList
    val m: ScalaMap[K, Set[V]] = xs.foldRight(map) { case ((key, set), map) =>
      val newEntry: (K, Set[V]) =
        map.get(key).fold((key, set))(s => (key, s ++ set))
      map + newEntry
    }
    new MapOfSet(m)
  }

  def ++(entries: (K, Iterable[V])): MapOfSet[K, V] = {
    val (key, values) = entries
    values.foldLeft(self) { case (acc, value) =>
      acc + ((key, value))
    }
  }

  def addAll(entries: (K, V)*): MapOfSet[K, V] =
    entries.foldLeft(self) { case (map, (k, v)) =>
      map + (k -> v)
    }

  def toOption: Option[MapOfSet[K, V]] =
    if (self.isEmpty) None else Some(self)

  override def iterator: Iterator[(K, Set[V])] = map.iterator
}
private[dynamodb] object MapOfSet {
  private[dynamodb] def apply[K, V](map: ScalaMap[K, Set[V]]) = new MapOfSet(map)
  def empty[K, V]: MapOfSet[K, V]                             = apply(ScalaMap.empty)
}
