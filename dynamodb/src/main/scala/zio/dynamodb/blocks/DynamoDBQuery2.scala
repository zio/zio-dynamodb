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

  // TODO: Avi - use TableName to differentiate overloading for now
  def get[A, From: SchemaCodec](tableName: String)(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[From, Either[ItemError, From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExpr(primaryKeyExpr)
    DynamoDBQuery.get(tableName, pkExpr.asAttrMap, SchemaCodec[From].projectionsFromSchema)
  }

  def update[A, From: SchemaCodec](tableName: String)(primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A])(
    action: Action[From]
  ): DynamoDBQuery[From, Option[From]] = {
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExpr(primaryKeyExpr)
    DynamoDBQuery
      .updateItem(tableName, pkExpr.asAttrMap)(action)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  def deleteFrom[A, From: SchemaCodec](
    tableName: String
  )(
    primaryKeyExpr: zio.blocks.schema.SchemaExpr[From, A]
  ): DynamoDBQuery[Any, Option[From]] = {
//    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExpr(primaryKeyExpr)
    val pkExpr: KeyConditionExpr.PrimaryKeyExpr[From] = BlocksApi.schemaExprToPrimaryKeyExpr(primaryKeyExpr)
    DynamoDBQuery
      .deleteItem(tableName, pkExpr.asAttrMap)
      .map(_.flatMap(item => DynamoDBQuery.fromItem(item).toOption))
  }

  /**
   * when executed will return a ZStream of A
   */
  def queryAll[A: SchemaCodec](
    tableName: String
    //keyConditionExpression: KeyConditionExpression, REVIEW: This is required by the dynamo API, should we make it required here?
  ): DynamoDBQuery[A, Stream[Throwable, A]] = DynamoDBQuery.queryAll(tableName)

}
