package zio.dynamodb

trait DynamoDBQueryFunctions {
  def getTable(tableName: TableName): DynamoDBTable =
    new DynamoDBTable {
      def get(
        key: AttrMap,
        readConsistency: ConsistencyMode = ConsistencyMode.Weak,
        capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
      )(projections: ProjectionExpression*): DynamoDBQuery[Option[AttrMap]] =
        DynamoDBQuery.GetItem(tableName, key, projections.toList, readConsistency, capacity)

    }
}
