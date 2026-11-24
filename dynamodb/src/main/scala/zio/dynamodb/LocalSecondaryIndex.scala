package zio.dynamodb

/** A local secondary index of a table, with its name, key schema and the attributes it projects. */
final case class LocalSecondaryIndex(
  indexName: String,
  keySchema: KeySchema,
  projection: ProjectionType
)
