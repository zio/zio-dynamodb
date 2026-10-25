/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import zio.blocks.chunk.Chunk
import scala.util.control.NoStackTrace

/**
 * Root of every typed error this library can raise in an interpreter's effect channel.
 * Extends `RuntimeException` (so it composes with each interpreter's own effect type — `Task`, CE's
 * `IO`, `Future` — and is caught by `case e: Exception` handlers, like the AWS SDK's own
 * exceptions) but mixes in `NoStackTrace`, since these are expected, structured failures
 * rather than exceptional JVM conditions — a stack trace would be noise, not signal.
 *
 * Sealed subtrees: [[DynamoDBError.ItemError]] (decoding and failed-condition errors),
 * [[DynamoDBError.ScanError]] (scan/query validation),
 * [[DynamoDBError.TransactionError]] (`transactWriteItems`/`transactGetItems` failures,
 * including [[DynamoDBError.TransactionError.TransactionCancelled]]'s per-item
 * [[DynamoDBError.CancellationReason]]s), and [[DynamoDBError.QueryBuilderError]] (a builder
 * modifier applied to a `DynamoDBQuery` variant that doesn't support it, e.g. `.where` on a
 * scan). Batch operations deliberately don't raise `DynamoDBError` for partial failure — see
 * `Batch.GetResult`/`Batch.WriteResult`'s errors-as-values design instead.
 */
sealed trait DynamoDBError extends RuntimeException with NoStackTrace with Product with Serializable {
  def message: String
  override def getMessage: String = message
}

/**
 * The four [[DynamoDBError]] subtrees ([[DynamoDBError.ItemError]],
 * [[DynamoDBError.ScanError]], [[DynamoDBError.TransactionError]],
 * [[DynamoDBError.QueryBuilderError]]) and their cases.
 */
object DynamoDBError {

  sealed trait ItemError extends DynamoDBError

  sealed trait ScanError extends DynamoDBError

  object ScanError {
    final case class ScanValidationError(message: String) extends ScanError
  }

  /**
   * Raised for a query built wrong: a modifier applied to an operation it doesn't affect (e.g.
   * `.where` on a `scan`, `.consistency` on a `putItem`), a `query` with no `.whereKey`, or a
   * query that can't go in a batch. The query still builds; the error is raised when it's run,
   * before any request is sent.
   */
  sealed trait QueryBuilderError extends DynamoDBError

  object QueryBuilderError {
    final case class UnsupportedModifier(message: String) extends QueryBuilderError
    final case class MissingKeyCondition(message: String) extends QueryBuilderError
    final case class UnbatchableQuery(message: String)    extends QueryBuilderError
    final case class InvalidPath(message: String)         extends QueryBuilderError
  }

  sealed trait TransactionError extends DynamoDBError

  object TransactionError {

    final case class TransactionValidationError(message: String) extends TransactionError

    /**
     * When raised by the library, the inherited `getCause` is the AWS SDK
     * `TransactionCanceledException` it was translated from.
     */
    final case class TransactionCancelled(
      reasons: Chunk[CancellationReason]
    ) extends TransactionError {
      def message: String = reasons.map(_.describe).mkString("; ")
    }
  }

  /**
   * One cancellation reason from a failed `TransactWriteItems` call. `item` is the item as it was, for
   * an action that returned it on failure (the default for conditional writes and condition checks).
   */
  final case class CancellationReason(
    code: CancellationCode,
    message: Option[String],
    item: Option[Item]
  ) {
    def describe: String = s"[${code.value}]${message.fold("")(m => s" $m")}"
  }

  /**
   * DynamoDB's documented transaction cancellation reason codes, with `Other` for any code added
   * later. `NoError` is the item-level `"None"` code: that action didn't cause the cancellation.
   */
  sealed abstract class CancellationCode(val value: String) extends Product with Serializable

  object CancellationCode {
    case object NoError                                extends CancellationCode("None")
    case object ConditionalCheckFailed                 extends CancellationCode("ConditionalCheckFailed")
    case object ItemCollectionSizeLimitExceeded        extends CancellationCode("ItemCollectionSizeLimitExceeded")
    case object TransactionConflict                    extends CancellationCode("TransactionConflict")
    case object ProvisionedThroughputExceeded          extends CancellationCode("ProvisionedThroughputExceeded")
    case object ThrottlingError                        extends CancellationCode("ThrottlingError")
    case object ValidationError                        extends CancellationCode("ValidationError")
    final case class Other(override val value: String) extends CancellationCode(value)

    private val known: Map[String, CancellationCode] =
      List(
        NoError,
        ConditionalCheckFailed,
        ItemCollectionSizeLimitExceeded,
        TransactionConflict,
        ProvisionedThroughputExceeded,
        ThrottlingError,
        ValidationError
      ).map(c => c.value -> c).toMap

    def fromString(code: String): CancellationCode = known.getOrElse(code, Other(code))
  }

  object ItemError {

    /**
     * A write's condition expression evaluated to false. `item` holds the item as it was when
     * the write asked for it, which conditional writes do by default. When raised by the
     * library, the inherited `getCause` is the AWS SDK `ConditionalCheckFailedException` it was
     * translated from.
     */
    final case class ConditionFailed(message: String, item: Option[Item]) extends ItemError

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
