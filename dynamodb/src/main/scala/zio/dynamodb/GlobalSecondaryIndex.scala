package zio.dynamodb

final case class GlobalSecondaryIndex(
  indexName: IndexName,
  keySchema: KeySchema,
  projection: ProjectionType,
  provisionedThroughput: Option[ProvisionedThroughput] = None
)
