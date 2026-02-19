package zio.dynamodb.blocks

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.{ DynamoDBQuery, KeyConditionExpr, SchemaCodec }

/** Entry point for Schema2 based API */
object DynamoDBQuery2 {

  def put[A: SchemaCodec](tableName: String, a: A): DynamoDBQuery[A, Option[A]] =
    DynamoDBQuery
      .putItem(tableName, DynamoDBQuery.toItem(a))
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))

  // TODO: Avi - use TableName to differentiate overloading for now
  def get[A, From: SchemaCodec](tableName: String)(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[From, Either[ItemError, From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExpr(primaryKeyExpr)
    DynamoDBQuery.get(tableName, pkExpr.asAttrMap, SchemaCodec[From].projectionsFromSchema)
  }

}
