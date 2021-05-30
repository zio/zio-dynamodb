package zio.dynamodb

final case class LocalSecondaryIndex(
  indexName: String,
  keySchema: KeySchema,
  projection: ProjectionType
)
