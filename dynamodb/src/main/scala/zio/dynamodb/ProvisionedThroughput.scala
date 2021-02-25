package zio.dynamodb

final case class ProvisionedThroughput(readCapacityUnit: Int, writeCapacityUnit: Int)
