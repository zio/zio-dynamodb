package zio.dynamodb

// TODO: could be have var args of Tuples?
final case class PrimaryKey(value: Map[String, AttributeValue])
