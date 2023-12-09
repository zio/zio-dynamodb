package zio.dynamodb

import scala.annotation.tailrec

private[dynamodb] final case class AliasMap private[dynamodb] (map: Map[AliasMap.Key, String], index: Int) { self =>

  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((AliasMap.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }

  private def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) = {
    def stripLeadingAndTrailingBackticks(s: String): String = // TODO: Avi - check with ARRAY syntax
      if (s.startsWith("`") && s.endsWith("`") && s.length > 1) s.substring(1, s.length - 1)
      else s

    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: (AliasMap, List[String])): (AliasMap, List[String]) =
      pe match {
        case ProjectionExpression.Root                                                 =>
          acc // identity
        case ProjectionExpression.MapElement(ProjectionExpression.Root, mapElementKey) =>
          val name  = stripLeadingAndTrailingBackticks(mapElementKey)
          val tuple = acc._1.map.get(AliasMap.PathSegment(ProjectionExpression.Root, name)) match {
            case Some(existingAlias) =>
              acc._1 -> (acc._2 :+ existingAlias)
            case None                =>
              val nameAlias = s"#n${acc._1.index}"
              val next      = AliasMap(
                acc._1.map + (AliasMap.PathSegment(ProjectionExpression.Root, name) -> nameAlias),
                acc._1.index + 1
              )
              val aliases   = acc._2 :+ nameAlias
              next -> aliases
          }

          loop(ProjectionExpression.Root, tuple)
        // we treat child map elements as a separate case in order to add a dot prefix to the alias
        case ProjectionExpression.MapElement(parent, mapElementKey)                    =>
          val aliasMapkey = AliasMap.PathSegment(parent, mapElementKey)
          val tuple       = acc._1.map.get(aliasMapkey) match {
            case Some(existingAlias) =>
              val aliases = (acc._2 :+ s".$existingAlias") // this is a child path, so we need a dot prefix
              acc._1 -> aliases
            case None                =>
              val nameAlias = s"#n${acc._1.index}"
              val next      = AliasMap(acc._1.map + (aliasMapkey -> nameAlias), acc._1.index + 1)
              val aliases = (acc._2 :+ s".$nameAlias") // this is a child path, so we need a dot prefix
              next -> aliases 
          }
          loop(parent, tuple)
        case ProjectionExpression.ListElement(parent, index)                           =>
          loop(parent, (acc._1, acc._2 :+ s"[$index]"))
      }

    val (aliasMap, xs) = loop(entry, (self, List.empty))
    aliasMap -> xs.reverse.mkString // last element is the root and does not have a leading dot
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(AliasMap.AttributeValueKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) =
    self.map
      .get(AliasMap.PathSegment(ProjectionExpression.Root, entry.toString))
      .map(varName => (self, varName))
      .getOrElse(self + entry)

  def ++(other: AliasMap): AliasMap = {
    val nextMap = self.map ++ other.map
    AliasMap(nextMap, nextMap.size)
  }

  def isEmpty: Boolean = self.index == 0
}

private[dynamodb] object AliasMap {

  sealed trait Key                                                                                extends Product with Serializable
  final case class AttributeValueKey(av: AttributeValue)                                          extends Key
  // we include parent to disambiguate PathSegment as a Map key for cases where the same segment name is used multiple times
  final case class PathSegment[From, To](parent: ProjectionExpression[From, To], segment: String) extends Key

  def empty: AliasMap = AliasMap(Map.empty, 0)
}
