package zio.dynamodb

final case class AttrMap(map: Map[String, AttributeValue])

object AttrMap extends GeneratedAttrMapApplies {

  val empty = new AttrMap(Map.empty)

}
