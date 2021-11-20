package zio.dynamodb

final case class AliasMap private (map: Map[AttributeValue, String], index: Int = 0) { self =>
  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((entry, variableAlias)), self.index + 1), variableAlias)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(entry).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def isEmpty: Boolean = self.index == 0
}

object AliasMap {
  def empty: AliasMap = AliasMap(Map.empty, 0)
}
