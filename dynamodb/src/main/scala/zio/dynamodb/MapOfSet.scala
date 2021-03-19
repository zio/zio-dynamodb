package zio.dynamodb

import scala.collection.immutable.{ Map => ScalaMap }

final case class MapOfSet[K, V](map: ScalaMap[K, Set[V]] = ScalaMap.empty) { self =>
  def +(entry: (K, V)): MapOfSet[K, V] = {
    val (key, value) = entry
    val newEntry     = self.map.get(key).fold((key, Set(value)))(set => (key, set + value))
    MapOfSet(self.map + newEntry)
  }
  def addAll(entries: (K, V)*): MapOfSet[K, V] =
    entries.foldLeft(self) {
      case (map, (k, v)) => map + (k -> v)
    }

  def ++(that: MapOfSet[K, V]): MapOfSet[K, V] = {
    val xs: Seq[(K, Set[V])]   = that.map.toList
    val m: ScalaMap[K, Set[V]] = xs.foldRight(map) {
      case ((key, set), map) =>
        val newEntry: (K, Set[V]) =
          map.get(key).fold((key, set))(s => (key, s ++ set))
        map + newEntry
    }
    MapOfSet(m)
  }
}
object MapOfSet                                                            {
  def empty[K, V]: MapOfSet[K, V] = MapOfSet(ScalaMap.empty)
}
