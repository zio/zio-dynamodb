package zio.dynamodb

trait DynamoDBTable {
  def get(
    key: PrimaryKey,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Option[Item]]

  def getAll(
    key: PrimaryKey
  ): DynamoDBQuery[Option[Item]] = get(key)()
}
