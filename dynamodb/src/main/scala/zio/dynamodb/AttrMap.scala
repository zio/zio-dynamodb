package zio.dynamodb

final case class AttrMap(map: Map[String, AttributeValue])

object AttrMap extends GeneratedAttrMapApplies {

  val empty: AttrMap = new AttrMap(Map.empty[String, AttributeValue])

}
