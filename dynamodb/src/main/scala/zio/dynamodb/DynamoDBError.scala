package zio.dynamodb

import software.amazon.awssdk.services.dynamodb.model.DynamoDbException

import scala.util.control.NoStackTrace
import zio.Chunk
import zio.NonEmptyChunk

/**
 * The error type of the effect that `DynamoDBQuery#execute` returns. It is a `Throwable` without a stack trace, and its
 * `getMessage` returns `DynamoDBError#message`. The subtypes group the errors by cause:
 *
 *  - `DynamoDBError.ItemError`: an item was not found or could not be decoded
 *  - `DynamoDBError.AWSError`: DynamoDB or the AWS SDK rejected a request
 *  - `DynamoDBError.UnknownNonFatalError`: any other non-fatal exception raised by a request
 *  - `DynamoDBError.BatchError`: a batched query could not be batched, or left items unprocessed
 *  - `DynamoDBError.TransactionError`: a query cannot run as a transaction
 *
 * To treat a failed condition on a write as a normal outcome, match on the AWS SDK exception inside `AWSError`:
 * {{{
 * import zio._
 * import zio.dynamodb.ProjectionExpression.\$
 * import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
 *
 * val insertOnce: ZIO[DynamoDBExecutor, DynamoDBError, Boolean] =
 *   DynamoDBQuery
 *     .putItem("people", Item("id" -> "1", "name" -> "Ann"))
 *     .where(\$("id").notExists)
 *     .execute
 *     .as(true)
 *     .catchSome { case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => ZIO.succeed(false) }
 * }}}
 *
 * @see [[https://zio.dev/zio-dynamodb/reference/error-handling/ Error Handling]]
 */
sealed trait DynamoDBError extends Throwable with NoStackTrace with Product with Serializable {

  /**
   * Describes the error. `getMessage` returns the same text.
   */
  def message: String
  override def getMessage(): String = message
}

/**
 * The subtypes of `DynamoDBError`.
 */
object DynamoDBError {

  /**
   * An error about one item: it was not found, or it could not be decoded into the requested type. The High Level
   * `DynamoDBQuery.get` returns it as a `Left` value instead of failing the effect; `zio.dynamodb.syntax.MaybeFound`
   * turns that `Either` into an `Option`.
   */
  sealed trait ItemError extends DynamoDBError

  /**
   * The two kinds of `DynamoDBError.ItemError`.
   */
  object ItemError {

    /**
     * No item was found. `DynamoDBQuery.get` returns it when no item has the requested primary key, with a message
     * such as `value with key AttrMap(Map(id -> String(1))) not found`. The in-memory executor of
     * `DynamoDBExecutor.test` also fails with it when a query names a table that does not exist.
     */
    final case class ValueNotFound(message: String) extends ItemError

    /**
     * An item could not be decoded into the requested type, for example because a field of the model is missing
     * (`field 'name' not found in AttributeValue map`), or because `DynamoDBQuery.getWithNarrow` found another case of
     * the sum type.
     */
    final case class DecodingError(message: String) extends ItemError
  }

  /**
   * Encapsulates the underlying AWS SDK dynamodb error in `cause` which can be pattern matched eg
   * `case DynamoDBError.AWSError(_: ConditionalCheckFailedException) => ...`
   */
  final case class AWSError(cause: DynamoDbException) extends DynamoDBError {
    override def message: String = cause.getMessage

  }

  /**
   * Encapsulates a non-fatal error (as defined by `scala.util.control.NonFatal`) that a request of
   * `DynamoDBExecutor.live` raised, other than an AWS SDK `DynamoDbException` or a `DynamoDBError`. A fatal error is
   * not wrapped: the effect dies. `message` is the message of `cause`, which can be `null`.
   */
  final case class UnknownNonFatalError(cause: Throwable) extends DynamoDBError {
    override def message: String = cause.getMessage
  }

  /**
   * You need to consider this error if queries result in batching eg if you are using `DynamoDBQuery.batch` or manually
   * `Zip`'ing together `DynamoDBQuery`'s or using utility functions that use `DynamoDBQuery.batch`. Note that when a
   * `WriteError` or a `GetError` is raised, the automatic retries of unprocessed items have already run. For a long
   * running process typical handler actions would be to record the errors and to carry on processing.
   */
  sealed trait BatchError extends DynamoDBError

  /**
   * The kinds of `DynamoDBError.BatchError`, and the unprocessed writes that a `WriteError` lists.
   */
  object BatchError {

    /**
     * A write that a `BatchWriteItem` request left unprocessed: a `Delete` or a `Put`.
     */
    sealed trait Write extends Product with Serializable

    /**
     * An unprocessed delete of the item with primary key `key`.
     */
    final case class Delete(key: PrimaryKey) extends Write

    /**
     * An unprocessed put of `item`.
     */
    final case class Put(item: Item) extends Write

    /**
     * Some puts or deletes of a `BatchWriteItem` request were still unprocessed after the retries. `unprocessedItems`
     * maps each table name to its unprocessed writes.
     */
    final case class WriteError(unprocessedItems: Map[String, Chunk[Write]]) extends BatchError {

      /**
       * Returns `unprocessed items returned by aws`.
       */
      val message = "unprocessed items returned by aws"
    }

    /**
     * Some keys of a `BatchGetItem` request were still unprocessed after the retries. `unprocessedKeys` maps each
     * table name to its unprocessed primary keys.
     */
    final case class GetError(unprocessedKeys: Map[String, Set[PrimaryKey]]) extends BatchError {

      /**
       * Returns `unprocessed keys returned by aws`.
       */
      val message = "unprocessed keys returned by aws"
    }

    /**
     * A query built with `DynamoDBQuery.batch` holds more than one query that cannot be batched, so `execute` sends
     * nothing. `reasons` lists how each query was classified, such as `multiple GetItem's`, `PutItem has a condition
     * expression` or `Query type UpdateItem not batchable`.
     */
    final case class UnbatchableQueryError(reasons: Set[String]) extends BatchError {

      /**
       * Returns `Query is not batchable for the following reasons: ` followed by `reasons` joined with commas.
       */
      val message = s"Query is not batchable for the following reasons: ${reasons.mkString(",")}"
    }
  }

  /**
   * You need to handle this error if you are using the transaction API ie `dynamoDBQuery.transaction` or
   * `dynamoDBQuery.safeTransaction`
   */
  sealed trait TransactionError extends DynamoDBError

  /**
   * The kinds of `DynamoDBError.TransactionError`.
   */
  object TransactionError {

    /**
     * The transaction has no actions. Queries built with the `DynamoDBQuery` constructors currently get
     * `InvalidTransactionActions` instead, for example a transaction over `DynamoDBQuery.batch` of an empty list.
     */
    case object EmptyTransaction extends TransactionError {

      /**
       * Returns `transaction is empty`.
       */
      val message = "transaction is empty"
    }

    /**
     * The transaction mixes gets with writes (puts, updates, deletes or condition checks). DynamoDB runs them in
     * separate requests, `TransactGetItems` and `TransactWriteItems`.
     */
    case object MixedTransactionTypes extends TransactionError {

      /**
       * Returns `transaction contains both get and write actions`.
       */
      val message = "transaction contains both get and write actions"
    }

    /**
     * The transaction holds a query that cannot run in a transaction, such as a scan, a query, a table operation or
     * `DynamoDBQuery.succeed`. `invalidActions` holds the first such query found.
     */
    final case class InvalidTransactionActions(invalidActions: NonEmptyChunk[DynamoDBQuery[Any, Any]])
        extends TransactionError {

      /**
       * Returns `transaction contains invalid actions`.
       */
      val message = "transaction contains invalid actions"
    }
  }

}
