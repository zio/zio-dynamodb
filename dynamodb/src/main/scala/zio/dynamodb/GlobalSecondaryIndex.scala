package zio.dynamodb

/**
 * A global secondary index of a table, with its name, key schema and the attributes it projects.
 * `provisionedThroughput` defaults to `None`, in which case no throughput is sent for the index.
 */
final case class GlobalSecondaryIndex(
  indexName: String,
  keySchema: KeySchema,
  projection: ProjectionType,
  provisionedThroughput: Option[ProvisionedThroughput] = None
)
