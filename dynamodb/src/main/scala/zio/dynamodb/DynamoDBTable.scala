package zio.dynamodb

trait DynamoDBTable {
  def get(
    key: AttrMap,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Option[Item]]

  def getAll(
    key: AttrMap
  ): DynamoDBQuery[Option[Item]] = get(key)()
}
