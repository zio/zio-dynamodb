package zio.dynamodb

trait DynamoDBQueryFunctions {
  def getTable(tableName: TableName): DynamoDBTable =
    new DynamoDBTable {
      def get(
        key: PrimaryKey,
        readConsistency: ConsistencyMode = ConsistencyMode.Weak,
        capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
      )(projections: ProjectionExpression*): DynamoDBQuery[Option[Item]] =
        DynamoDBQuery.GetItem(key, tableName, readConsistency, projections.toList, capacity)

    }
}
