package zio.dynamodb.blocks

import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.UpdateExpression.Action
import zio.dynamodb.{ DynamoDBQuery, KeyConditionExpr, SchemaCodec }
import zio.stream.Stream

/** Entry point for Schema2 based API */
object DynamoDBQuery2 {

  def put[A: SchemaCodec](tableName: String, a: A): DynamoDBQuery[A, Option[A]] =
    DynamoDBQuery
      .putItem(tableName, DynamoDBQuery.toItem(a))
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))

  def get[A, From: SchemaCodec](tableName: String)(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[From, Either[ItemError, From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery.get(tableName, pkExpr.asAttrMap, SchemaCodec[From].projectionsFromSchema)
  }

  def update[A, From: SchemaCodec](tableName: String)(primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A])(
    action: Action[From]
  ): DynamoDBQuery[From, Option[From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery
      .updateItem(tableName, pkExpr.asAttrMap)(action)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  def deleteFrom[A, From: SchemaCodec](
    tableName: String
  )(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[Any, Option[From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExprUnsafe(primaryKeyExpr)
    DynamoDBQuery
      .deleteItem(tableName, pkExpr.asAttrMap)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  /**
   * when executed will return a ZStream of A
   */
  def queryAll[A: SchemaCodec](
    tableName: String
  ): DynamoDBQuery[A, Stream[Throwable, A]] = DynamoDBQuery.queryAll(tableName)

}
