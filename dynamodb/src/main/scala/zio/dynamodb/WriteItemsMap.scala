package zio.dynamodb

import zio.dynamodb.DynamoDBQuery.BatchWriteItem

import scala.collection.immutable.Map

final case class WriteItemsMap(map: Map[TableName, Set[BatchWriteItem.Write]] = Map.empty) { self =>
  def +(entry: (TableName, BatchWriteItem.Write)): WriteItemsMap = {
    val newEntry =
      map.get(entry._1).fold((entry._1, Set(entry._2)))(set => (entry._1, set + entry._2))
    WriteItemsMap(map + newEntry)
  }
  def ++(that: WriteItemsMap): WriteItemsMap = {
    val xs: Seq[(TableName, Set[BatchWriteItem.Write])] = that.map.toList
    val m: Map[TableName, Set[BatchWriteItem.Write]]    = xs.foldRight(map) {
      case ((tableName, set), map) =>
        val newEntry: (TableName, Set[BatchWriteItem.Write]) =
          map.get(tableName).fold((tableName, set))(s => (tableName, s ++ set))
        map + newEntry
    }
    WriteItemsMap(m)
  }
}
object WriteItemsMap                                                                       {
  def empty = WriteItemsMap(Map.empty)
}
