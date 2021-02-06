package zio.dynamodb

final case class PrimaryKey(value: Map[String, AttributeValue])
