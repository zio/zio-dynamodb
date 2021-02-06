package zio.dynamodb

import zio.Chunk

trait DynamoDBTable {
  def get(
    key: PrimaryKey,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Chunk[Byte]]

  def getAll(
    key: PrimaryKey
  ): DynamoDBQuery[Chunk[Byte]] = get(key)()
}
