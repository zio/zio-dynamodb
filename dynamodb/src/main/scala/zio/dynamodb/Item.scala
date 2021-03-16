package zio.dynamodb

final case class Item(value: Map[String, AttributeValue])
