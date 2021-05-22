package zio

import zio.dynamodb.DynamoDBExecutor.DynamoDBExecutor

package object dynamodb {
  // Filter expression is the same as a ConditionExpression but when used with Query but does not allow key attributes
  type FilterExpression = ConditionExpression
  type LastEvaluatedKey = Option[AttrMap]

  private[dynamodb] def ddbExecute[A](query: DynamoDBQuery[A]): ZIO[DynamoDBExecutor, Exception, A] =
    ZIO.accessM[DynamoDBExecutor](_.get.execute(query))

}
