package zio.dynamodb

import zio.blocks.chunk.Chunk
import scala.util.control.NoStackTrace

sealed trait DynamoDBError extends Throwable with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage: String = message
}

object DynamoDBError {

  sealed trait ItemError extends DynamoDBError

  sealed trait ScanError extends DynamoDBError

  object ScanError {
    final case class ScanValidationError(message: String) extends ScanError
  }

  sealed trait TransactionError extends DynamoDBError

  object TransactionError {

    final case class TransactionValidationError(message: String) extends TransactionError

    final case class TransactionCancelled(
      reasons: Chunk[CancellationReason]
    ) extends TransactionError {
      def message: String = reasons.map(_.describe).mkString("; ")
    }
  }

  /**
   * One cancellation reason from a failed `TransactWriteItems` call.
   *  `item` is populated only when `ReturnValuesOnConditionCheckFailure.AllOld` was set
   *  on the corresponding action.
   */
  final case class CancellationReason(
    code: String,
    message: Option[String],
    item: Option[Item]
  ) {
    def describe: String = s"[$code]${message.fold("")(m => s" $m")}"
  }

  object ItemError {

    /**
     * Signals that an expected key was absent from the DynamoDB response.
     *  Produced by `DynamoDBQuery.get` when `getItem` returns `None`, and
     *  available for use with `DynamoDBQuery.Absolve` to promote a missing-item
     *  `Option` into the effect error channel. Not a decoding failure —
     *  it indicates the item simply does not exist in the table.
     */
    final case class ValueNotFound(message: String) extends ItemError

    /**
     * Composite decoding failure: one or more [[DecodingError.Cause]] entries.
     *
     *  Backed by `Array[Cause]` so the codec deriver can convert an
     *  `ArrayBuffer[Cause]` to a `DecodingError` with a single `toArray` call
     *  and no cons-cell allocation.  Use `++` to merge two errors; use the
     *  companion factory methods to construct single-failure instances.
     */
    final class DecodingError private ( // TODO: should this be public????
      private[dynamodb] val underlying: Array[DecodingError.Cause]
    ) extends ItemError
        with Product
        with Serializable {

      def causes: Array[DecodingError.Cause] = underlying

      def message: String = underlying.iterator.map(_.message).mkString("; ")

      def ++(other: DecodingError): DecodingError =
        new DecodingError(underlying ++ other.underlying)

      // Product — required because DynamoDBError extends Product
      def canEqual(other: Any): Boolean  = other.isInstanceOf[DecodingError]
      def productArity: Int              = 1
      def productElement(n: Int): Any    = n match {
        case 0 => underlying
        case _ => throw new IndexOutOfBoundsException(n.toString)
      }
      override def productPrefix: String = "DecodingError"
      override def toString: String      = s"DecodingError($message)"
    }

    object DecodingError {

      /** One contributing cause within a composite [[DecodingError]]. */
      sealed trait Cause { def message: String }

      /** A required field was absent from the DynamoDB item map. */
      final case class MissingField(field: String) extends Cause {
        def message: String = s"missing field '$field'"
      }

      /** An attribute value had the wrong DynamoDB type. */
      final case class TypeMismatch(field: String, expected: String, got: String) extends Cause {
        def message: String = s"field '$field': expected $expected, got $got"
      }

      /** General decode failure (catch-all for errors without a dedicated type). */
      final case class Failure(message: String) extends Cause

      // -- Factories ---------------------------------------------------------

      /** Single free-form message — used by LL codecs and backward-compat sites. */
      def apply(message: String): DecodingError =
        new DecodingError(Array(Failure(message)))

      /**
       * Build from a pre-populated array — used by the codec deriver to finalize
       *  an `ArrayBuffer[Cause]` in one `toArray` call.
       */
      def apply(causes: Array[Cause]): DecodingError =
        new DecodingError(causes)

      def failure(detail: String): DecodingError =
        new DecodingError(Array(Failure(detail)))

      def missingField(field: String): DecodingError =
        new DecodingError(Array(MissingField(field)))

      def typeMismatch(
        field: String,
        expected: String,
        got: String
      ): DecodingError =
        new DecodingError(Array(TypeMismatch(field, expected, got)))
    }
  }
}
