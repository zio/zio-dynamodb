package zio.dynamodb

import zio.Chunk

trait DynamoDBQueryFunctions {
  def getTable(tableName: TableName): DynamoDBTable =
    new DynamoDBTable {
      def get(
        key: PrimaryKey,
        readConsistency: ConsistencyMode = ConsistencyMode.Weak,
        capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
      )(projections: ProjectionExpression*): DynamoDBQuery[Chunk[Byte]] =
        DynamicDBQuery.GetItem(key, tableName, readConsistency, projections.toList, capacity)

    }
}
