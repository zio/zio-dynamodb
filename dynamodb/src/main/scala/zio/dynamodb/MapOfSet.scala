package zio.dynamodb

import scala.collection.immutable.{ Map => ScalaMap }

final case class MapOfSet[K, V](map: ScalaMap[K, Set[V]] = ScalaMap.empty) { self =>
  def +(entry: (K, V)): MapOfSet[K, V] = {
    val newEntry =
      map.get(entry._1).fold((entry._1, Set(entry._2)))(set => (entry._1, set + entry._2))
    MapOfSet(map + newEntry)
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
