package zio.dynamodb

trait DynamoDBTable {
  def get(
    key: PrimaryKey,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Item]

  // TODO: how about `getAllAttributes`?
  def getAll(
    key: PrimaryKey
  ): DynamoDBQuery[Item] = get(key)()
}
