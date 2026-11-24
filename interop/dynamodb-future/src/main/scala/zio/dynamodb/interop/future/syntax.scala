package zio.dynamodb.interop.future

import zio.CancelableFuture

/** Adds `executeToF` to a `DynamoDBQuery` for running it as a `Future`. */
object syntax {

  /** Adds `executeToF` to a `DynamoDBQuery`. */
  implicit class ZioDynamoDBQueryOps[In, Out](val query: zio.dynamodb.DynamoDBQuery[In, Out]) extends AnyVal {

    /**
     * Runs this query with the implicit `DynamoDBExecutorF` and returns its result as a `CancelableFuture`. See
     * `DynamoDBExecutorF#execute`.
     */
    def executeToF(implicit ev: DynamoDBExecutorF): CancelableFuture[Out] =
      ev.execute(query)
  }

}
