package zio.dynamodb

/** The read and write capacity units of a table or a global secondary index. */
final case class ProvisionedThroughput(readCapacityUnit: Long, writeCapacityUnit: Long)
