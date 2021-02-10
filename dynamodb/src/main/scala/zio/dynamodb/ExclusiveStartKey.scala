package zio.dynamodb

final case class ExclusiveStartKey(value: Map[String, AttributeValue])
