package zio.dynamodb

// map is AV -> aliasName
// index is the next index to use
// alias name s":v${self.index}
private[dynamodb] final case class AliasMap private (map: Map[AttributeValue, String], index: Int) { self =>
  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((entry, variableAlias)), self.index + 1), variableAlias)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(entry).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def ++(other: AliasMap): AliasMap = {
    val nextMap = self.map ++ other.map
    AliasMap(nextMap, nextMap.size)
  }

  def isEmpty: Boolean = self.index == 0
}

private[dynamodb] object AliasMap {
  def empty: AliasMap = AliasMap(Map.empty, 0)
}
