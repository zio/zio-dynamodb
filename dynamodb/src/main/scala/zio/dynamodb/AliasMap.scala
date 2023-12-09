package zio.dynamodb

import scala.annotation.tailrec

private[dynamodb] final case class AliasMap private[dynamodb] (map: Map[AliasMap.Key, String], index: Int) { self =>

  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((AliasMap.AttributeValueKey(entry), variableAlias)), self.index + 1), variableAlias)
  }

  private def +[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) = {
    println(s"XXXXX AliasMap.+ pe = $entry")
    def stripLeadingAndTrailingBackticks(s: String): String = // TODO: Avi - check with ARRAY syntax
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
          val map       = acc._1.map + ((AliasMap.PathSegment(ProjectionExpression.Root, name), nameAlias))
          val xs        = acc._2 :+ nameAlias
          val t         = (AliasMap(map, acc._1.index + 1), xs)
          // val t2        = // do not overwite an existing alias
          //   if (acc._1.map.get(AliasMap.PathSegment(ProjectionExpression.Root, name)).isDefined) {
          //     println(s"XXXXXX SKIPPING")
          //     acc
          //   } else {
          //     println(s"XXXXXX adding accumulator $t")
          //     t
          //   }
          val t3        = acc._1.map.get(AliasMap.PathSegment(ProjectionExpression.Root, name)) match {
            case Some(aliasName) =>
              println(s"XXXXXX SKIPPING")
              val t = (acc._1, acc._2 :+ aliasName)
              println(s"XXXXXX SKIPPING tuple = $t")
              t
            case None            =>
              t
          }
          println(s"XXXXX AliasMap.+ MapElement at Root found = $found name = $name tuple = $t3")

          loop(ProjectionExpression.Root, t3)
        case ProjectionExpression.MapElement(parent, mapElementKey)            =>
          // println(s"XXXXX AliasMap.+ MapElement parent = $parent, key = $mapElementKey")
          // val nameAlias = s"#n${acc._1.index}"
          // val next      = AliasMap(acc._1.map + ((AliasMap.PathSegment(parent, mapElementKey), nameAlias)), acc._1.index + 1)
          // loop(parent, (next, acc._2 :+ ("." + nameAlias)))
          println(s"XXX AliasMap.+ MapElement parent = $parent, key = $mapElementKey")
          val key       = AliasMap.PathSegment(parent, mapElementKey)
          val nameAlias = s"#n${acc._1.index}"
          val t2        = acc._1.map.get(key) match {
            case Some(existingAlias) =>
              val tuple = (acc._1, acc._2 :+ s".$existingAlias") // this is a child path, so we need a dot prefix
              println(s"XXX AliasMap.+ MapElement found existing alias = $existingAlias tuple = $tuple")
              tuple
            case None                =>
              val next  = AliasMap(acc._1.map + ((key, nameAlias)), acc._1.index + 1) 
              val tuple = (next, acc._2 :+ s".$nameAlias") // this is a child path, so we need a dot prefix
              println(s"XXX AliasMap.+ MapElement adding new alias = $nameAlias tuple = $tuple")
              tuple
          }
          loop(parent, t2)
        case ProjectionExpression.ListElement(parent, index)                   =>
          loop(parent, (acc._1, acc._2 :+ s"[$index]"))
      }

    val (aliasMap, xs) = loop(entry, (self, List.empty))
    println(s"XXXXXX final aliasMap = $aliasMap, xs = $xs")
    (aliasMap, xs.reverse.mkString) // last element is the root and does not have a leading dot 
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(AliasMap.AttributeValueKey(entry)).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def getOrInsert[From, To](entry: ProjectionExpression[From, To]): (AliasMap, String) = {
    println(s"XXXXXXXXXX AliasMap.getOrInsert entry = $entry")
    println(s"XXXXXXXXXX self.map = ${self.map}")
    self.map
      .get(AliasMap.PathSegment(ProjectionExpression.Root, entry.toString))
      .map(varName => (self, varName))
      .getOrElse {
        val m = self + entry
        println(s"XXXXXXXXXX entry not found, adding to map. Map = ${m._1.map}}")
        m
      }
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

  def empty: AliasMap = AliasMap(Map.empty, 0)
}
