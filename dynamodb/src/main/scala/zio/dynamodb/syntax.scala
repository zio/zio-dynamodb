package zio.dynamodb

import zio.ZIO

/**
 * Extension methods for the effects that `DynamoDBQuery#execute` returns. Import `zio.dynamodb.syntax._` to use them.
 */
object syntax {

  /**
   * Adds `maybeFound` to an effect whose result is an `Either[DynamoDBError.ItemError, A]`, such as the High Level
   * `DynamoDBQuery.get` after `execute`.
   */
  implicit class MaybeFound[R, A](zio: ZIO[R, DynamoDBError, Either[DynamoDBError.ItemError, A]]) {

    /**
     * Moves `Left[ItemError.DecodingError]` to the error channel and returns `Some(a)` if the item is found else
     * `None`:
     * {{{
     * import zio._
     * import zio.dynamodb.syntax._
     * import zio.schema.{ DeriveSchema, Schema }
     *
     * final case class Person(id: String, name: String)
     * object Person {
     *   implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
     *   val (id, name) = ProjectionExpression.accessors[Person]
     * }
     *
     * val found: ZIO[DynamoDBExecutor, DynamoDBError, Option[Person]] =
     *   DynamoDBQuery.get("people")(Person.id.partitionKey === "1").execute.maybeFound
     * }}}
     */
    def maybeFound: ZIO[R, DynamoDBError, Option[A]] =
      zio.flatMap {
        case Left(e @ DynamoDBError.ItemError.DecodingError(_)) => ZIO.fail(e)
        case Left(DynamoDBError.ItemError.ValueNotFound(_))     => ZIO.succeed(None)
        case Right(a)                                           => ZIO.succeed(Some(a))
      }
  }

}
