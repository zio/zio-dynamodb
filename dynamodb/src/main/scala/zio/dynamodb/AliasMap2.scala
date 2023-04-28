package zio.dynamodb

import scala.annotation.tailrec

private[dynamodb] final case class AliasMap2 private[dynamodb] (map: Map[AliasMap2.Key, String], index: Int) { self =>
  private def +(entry: AttributeValue): (AliasMap2, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap2(self.map + ((AliasMap2.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }

  private def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) = {
    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: (AliasMap2, List[String])): (AliasMap2, List[String]) =
      pe match {
        case ProjectionExpression.Root                                        =>
          acc // identity
        case ProjectionExpression.MapElement(ProjectionExpression.Root, name) =>
          val nameAlias = s"#n${acc._1.index}"
          val mapTmp    = acc._1.map + ((AliasMap2.PathSegment(ProjectionExpression.Root, name), nameAlias))
          val xs        = acc._2 :+ nameAlias
          val map       = mapTmp + ((AliasMap2.FullPath(pe), xs.reverse.mkString)) // cache final result
          (AliasMap2(map, self.index + 1), xs)
        case ProjectionExpression.MapElement(parent, key)                     =>
          val nameAlias = s"#n${acc._1.index}"
          val next      = AliasMap2(acc._1.map + ((AliasMap2.PathSegment(parent, key), nameAlias)), acc._1.index + 1)
          loop(parent, (next, acc._2 :+ ("." + key)))
        case ProjectionExpression.ListElement(parent, index)                  =>
          loop(parent, (acc._1, acc._2 :+ s"[$index]"))
      }

    val (aliasMap, xs) = loop(entry, (self, List.empty))
    (aliasMap, xs.reverse.mkString)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap2, String) =
    self.map.get(AliasMap2.AttributeValueKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) =
    self.map.get(AliasMap2.FullPath(entry)).map(varName => (self, varName)).getOrElse {
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
  final case class AttributeValueKey(av: AttributeValue)                                          extends Key
  // we include parent to disambiguate PathSegment as a Map key for cases where the same segment name is used multiple times
  final case class PathSegment[From, To](parent: ProjectionExpression[From, To], segment: String) extends Key
  // used to cache the final substituted path - is this necessary????
  final case class FullPath[From, To](path: ProjectionExpression[From, To])                       extends Key

  def empty: AliasMap2 = AliasMap2(Map.empty, 0)
}
