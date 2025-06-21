package zio.dynamodb.interop.future

import zio.CancelableFuture

object syntax {
  implicit class ZioDynamoDBQueryOps[In, Out](val query: zio.dynamodb.DynamoDBQuery[In, Out]) extends AnyVal {
    def executeToF(implicit ev: DynamoDBExecutorF): CancelableFuture[Out] =
      ev.execute(query)
  }

}
