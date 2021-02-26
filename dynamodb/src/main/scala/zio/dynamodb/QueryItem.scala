package zio.dynamodb

final case class QueryItem(item: Item, lastEvaluatedKey: Option[PrimaryKey])
