package zio.dynamodb

trait DynamoDBTable {
  def get(
    key: AttrMap,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Option[AttrMap]]

  def getAll(
    key: AttrMap
  ): DynamoDBQuery[Option[AttrMap]] = get(key)()
}
