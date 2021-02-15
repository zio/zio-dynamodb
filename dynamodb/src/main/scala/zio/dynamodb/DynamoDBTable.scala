package zio.dynamodb

trait DynamoDBTable {
  def get(
    key: PrimaryKey,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Item]

  def getAll( // TODO: how about `getAllAttributes` ? to be clear we are talking about the axis of attributes rather than axis of rows?
    key: PrimaryKey
  ): DynamoDBQuery[Item] = get(key)()
}
