package zio.dynamodb

import scala.collection.immutable.HashSet

private[dynamodb] object ReservedAttributeNames {
  val reservedWords: Set[String] = HashSet("FILTER", "FLOAT", "TTL")
  val Prefix: String             = "~~~~~~~~~~~~"

  def escape(pathSegment: String): String = {
    println(s"XXXXXXXXXXXXXXXXXXXXXXX pathSegment='$pathSegment'")
    if (reservedWords.contains(pathSegment.toUpperCase)) s"#$Prefix$pathSegment" else pathSegment
  }

}
