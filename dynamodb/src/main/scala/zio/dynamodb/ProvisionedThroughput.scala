package zio.dynamodb

final case class ProvisionedThroughput(readCapacityUnit: Long, writeCapacityUnit: Long)
