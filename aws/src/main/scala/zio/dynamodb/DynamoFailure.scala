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

import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.{ NonRetryableException, SdkClientException }
import software.amazon.awssdk.services.dynamodb.model.{
  ConditionalCheckFailedException,
  ItemCollectionSizeLimitExceededException,
  ResourceNotFoundException,
  TransactionConflictException
}

import scala.util.control.NoStackTrace

/**
 * Every failure of a query, grouped by what the caller should do next. Each case keeps the
 * original exception as its `cause`. Total: anything not classified lands in `Other`.
 */
sealed abstract class DynamoFailure(val cause: Throwable)
    extends Throwable(cause.getMessage, cause)
    with NoStackTrace
    with Product
    with Serializable

object DynamoFailure {

  /** A write's condition evaluated to false; `item` is set when `AllOld` was requested. */
  final case class ConditionFailed(item: Option[Item], error: Throwable) extends DynamoFailure(error)

  /** A failure the library already models as a typed error (e.g. a cancelled transaction). */
  final case class Library(error: DynamoDBError) extends DynamoFailure(error)

  /** Throttled past the library's retries or retry quota: back off, re-queue, shed load. */
  final case class Throttled(error: AwsServiceException) extends DynamoFailure(error)

  /** Write conflicted with a transaction or another region's write: retry later. */
  final case class Conflict(error: AwsServiceException) extends DynamoFailure(error)

  /** Couldn't reach the service, timed out, or got a 5xx: retry later or fail over. */
  final case class Unavailable(error: Throwable) extends DynamoFailure(error)

  /** The request or the data is the problem; retrying won't help. */
  final case class Rejected(error: AwsServiceException) extends DynamoFailure(error)

  final case class Other(error: Throwable) extends DynamoFailure(error)

  private val serverErrorStatusCodes: Set[Int] = Set(500, 502, 503, 504)

  // Matched by code: the exception class only exists in newer SDK versions than this module pins.
  private def errorCode(e: AwsServiceException): String =
    Option(e.awsErrorDetails).map(_.errorCode).orNull

  private[dynamodb] def itemOf(e: ConditionalCheckFailedException): Option[Item] =
    if (e.hasItem && !e.item.isEmpty) Some(AwsCodecs.fromAwsItem(e.item)) else None

  def unwrap(t: Throwable): Throwable =
    t match {
      case f: DynamoFailure => f.cause
      case _                => t
    }

  def classify(t: Throwable): DynamoFailure =
    t match {
      case f: DynamoFailure                                                             => f
      case e: DynamoDBError.ItemError.ConditionFailed                                   => ConditionFailed(e.item, e)
      case e: ConditionalCheckFailedException                                           => ConditionFailed(itemOf(e), e)
      case e: DynamoDBError                                                             => Library(e)
      case e: AwsServiceException if e.isThrottlingException                            => Throttled(e)
      case e: TransactionConflictException                                              => Conflict(e)
      case e: AwsServiceException if errorCode(e) == "ReplicatedWriteConflictException" => Conflict(e)
      case e: AwsServiceException if serverErrorStatusCodes.contains(e.statusCode)      => Unavailable(e)
      case e: ItemCollectionSizeLimitExceededException                                  => Rejected(e)
      case e: ResourceNotFoundException                                                 => Rejected(e)
      case e: AwsServiceException if e.statusCode == 400                                => Rejected(e)
      case e: NonRetryableException                                                     => Other(e)
      case e: SdkClientException                                                        => Unavailable(e)
      case e                                                                            => Other(e)
    }
}
