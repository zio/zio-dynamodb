package zio.dynamodb

final case class GlobalSecondaryIndex(
  indexName: String,
  keySchema: KeySchema,
  projection: ProjectionType,
  provisionedThroughput: Option[ProvisionedThroughput] = None
)
