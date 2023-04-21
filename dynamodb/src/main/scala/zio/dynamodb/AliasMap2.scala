package zio.dynamodb

import scala.annotation.tailrec

/*
  private[dynamodb] def toStringEscaped(escape: Boolean): String = {
    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: List[String]): List[String] =
      pe match {
        case Root                                        =>
          acc // identity
        case ProjectionExpression.MapElement(Root, name) =>
          val pathSegment = if (escape) ExpressionAttributeNames.escape(name) else name
          acc :+ pathSegment
        case MapElement(parent, key)                     =>
          val pathSegment = if (escape) ExpressionAttributeNames.escape(key) else key
          loop(parent, acc :+ "." + pathSegment)
        case ListElement(parent, index)                  =>
          loop(parent, acc :+ s"[$index]")
      }

    loop(self, List.empty).reverse.mkString("")
  }
 */

private[dynamodb] final case class AliasMap2 private[dynamodb] (map: Map[AliasMap2.Key, String], index: Int) { self =>
  private def +(entry: AttributeValue): (AliasMap2, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap2(self.map + ((AliasMap2.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }
  // def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) = {
  //   val variableAlias = s"#n${self.index}"
  //   (AliasMap2(self.map + ((AliasMap2.FullPath(entry), variableAlias)), self.index + 1), variableAlias)
  // }
  def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap2, String) = {
    @tailrec
    def loop(pe: ProjectionExpression[_, _], acc: (AliasMap2, List[String])): (AliasMap2, List[String]) =
      pe match {
        case ProjectionExpression.Root                                        =>
          // TODO: anything we need to do here?????
          acc // identity
        case ProjectionExpression.MapElement(ProjectionExpression.Root, name) =>
          //acc :+ name
          val nameAlias = s"#n${acc._1.index}"
          // we a map of "PE -> final subst path" to cache final result
          // we also need to maintain a map of pathElement -> substPathElement
          val next      = AliasMap2(acc._1.map + ((AliasMap2.PathElement(name), nameAlias)), acc._1.index + 1)
          val mapTmp    = acc._1.map + ((AliasMap2.PathElement(name), nameAlias))
          val xs        = acc._2 :+ nameAlias
          val map       = mapTmp + ((AliasMap2.FullPath(pe), xs.reverse.mkString(""))) // cache final result
          (AliasMap2(map, self.index + 1), xs)
        case ProjectionExpression.MapElement(parent, key)                     =>
//          loop(parent, acc :+ "." + key)
          val nameAlias = s"#n${acc._1.index}"
          val next      = AliasMap2(acc._1.map + ((AliasMap2.PathElement(key), nameAlias)), acc._1.index + 1)
          // we need to add state (pathElement -> substPathElement) to acc._1
          loop(parent, (next, acc._2 :+ ("." + key)))
        case ProjectionExpression.ListElement(parent, index)                  =>
//          loop(parent, acc :+ s"[$index]")
          // we need to add state (pathElement -> substPathElement) to acc._1
          loop(parent, (acc._1, acc._2 :+ s"[$index]")) // OK - no subst happens for array indexes!!!!
      }

//    loop(entry, (self, List.empty)).reverse.mkString("") // TODO: map over list and reverse and mkString
    val (aliasMap, xs) = loop(entry, (self, List.empty))
    (aliasMap, xs.reverse.mkString(""))
  }

  def getOrInsert(entry: AttributeValue): (AliasMap2, String)                           =
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
  final case class AttributeValueKey(av: AttributeValue)                    extends Key
  final case class PathElement(element: String)                             extends Key
  // used to cache the final substituted path - is this necessary????
  final case class FullPath[From, To](path: ProjectionExpression[From, To]) extends Key

  def empty: AliasMap2 = AliasMap2(Map.empty, 0)
}
