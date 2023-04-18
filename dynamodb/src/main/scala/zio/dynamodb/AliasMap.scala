package zio.dynamodb

private[dynamodb] final case class AliasMap2 private[dynamodb] (map: Map[AliasMap2.Key, String], index: Int) { self =>
  private def +(entry: AttributeValue): (AliasMap2, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap2(self.map + ((AliasMap2.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }
  def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) = {
    val variableAlias = s"#n${self.index}"
    (AliasMap2(self.map + ((AliasMap2.PathKey(entry), variableAlias)), self.index + 1), variableAlias)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap2, String) =
    self.map.get(AliasMap2.AttributeValueKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }
  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) =
    self.map.get(AliasMap2.PathKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def ++(other: AliasMap2): AliasMap2 = {
    val nextMap = self.map ++ other.map
    AliasMap2(nextMap, nextMap.size)
  }

  def isEmpty: Boolean = self.index == 0
}

private[dynamodb] object AliasMap2 {

  sealed trait Key
  final case class AttributeValueKey(av: AttributeValue)                   extends Key
  final case class PathKey[From, To](path: ProjectionExpression[From, To]) extends Key

  def empty: AliasMap2 = AliasMap2(Map.empty, 0)
}
