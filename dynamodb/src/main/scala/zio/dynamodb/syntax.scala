package zio.dynamodb

import zio.ZIO

object syntax {
  implicit class MaybeFound[R, A](zio: ZIO[R, DynamoDBError, Either[DynamoDBError.ItemError, A]]) {

    /**
     * Moves Left[ItemError.DecodingError] to the error channel and returns Some(a) if the item is found else None
     * eg {{{ DynamoDBQuery.get("table")(Person.id.partitionKey === 1).execute.maybeFound }}}
     */
    def maybeFound: ZIO[R, DynamoDBError, Option[A]] =
      zio.flatMap {
        case Left(e @ DynamoDBError.ItemError.DecodingError(_)) => ZIO.fail(e)
        case Left(DynamoDBError.ItemError.ValueNotFound(_))     => ZIO.succeed(None)
        case Right(a)                             => ZIO.succeed(Some(a))
      }
  }
  
}
