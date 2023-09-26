package zio.dynamodb

import scala.annotation.tailrec

private[dynamodb] final case class AliasMap private[dynamodb] (map: Map[AliasMap.Key, String], index: Int) { self =>

  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((AliasMap.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }

  private def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) = {
    def stripLeadingAndTrailingBackticks(s: String): String =
      if (s.startsWith("`") && s.endsWith("`") && s.length > 1) s.substring(1, s.length - 1)
      else s

    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: (AliasMap, List[String])): (AliasMap, List[String]) =
      pe match {
        case ProjectionExpression.Root                                         =>
          acc // identity
        case ProjectionExpression.MapElement(ProjectionExpression.Root, found) =>
          val name      = stripLeadingAndTrailingBackticks(found)
          val nameAlias = s"#n${acc._1.index}"
          val mapTmp    = acc._1.map + ((AliasMap.PathSegment(ProjectionExpression.Root, name), nameAlias))
          val xs        = acc._2 :+ nameAlias
          val map       = mapTmp + ((AliasMap.FullPath(entry), xs.reverse.mkString)) // cache final result
          val t         = (AliasMap(map, acc._1.index + 1), xs)
          val t2        = // do not overwite an existing alias
            if (acc._1.map.get(AliasMap.PathSegment(ProjectionExpression.Root, name)).isDefined)
              acc
            else
              t
          loop(ProjectionExpression.Root, t2)
        case ProjectionExpression.MapElement(parent, key)                      =>
          val nameAlias = s"#n${acc._1.index}"
          val next      = AliasMap(acc._1.map + ((AliasMap.PathSegment(parent, key), nameAlias)), acc._1.index + 1)
          loop(parent, (next, acc._2 :+ ("." + nameAlias)))
        case ProjectionExpression.ListElement(parent, index)                   =>
          loop(parent, (acc._1, acc._2 :+ s"[$index]"))
      }

    val (aliasMap, xs) = loop(entry, (self, List.empty))
    (aliasMap, xs.reverse.mkString)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(AliasMap.AttributeValueKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) =
    self.map
      .get(AliasMap.PathSegment(ProjectionExpression.Root, entry.toString))
      .map(varName => (self, varName))
      .getOrElse {
        self + entry
      }

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
  // used to cache the final substituted path
  final case class FullPath[From, To](path: ProjectionExpression[From, To])                       extends Key

  def empty: AliasMap = AliasMap(Map.empty, 0)
}
