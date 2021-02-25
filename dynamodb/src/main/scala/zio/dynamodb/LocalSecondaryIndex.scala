package zio.dynamodb

final case class LocalSecondaryIndex(
  indexName: IndexName,
  keySchema: KeySchema,
  projection: ProjectionType
)
