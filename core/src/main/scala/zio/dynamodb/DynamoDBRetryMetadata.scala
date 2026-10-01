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

/**
 * Identity for a retried operation, passed to [[RetryInterceptor.onRetry]]. Built directly from
 *  the originating [[DynamoDBQuery]] at the retry call site — no response involved (the call
 *  hasn't succeeded yet), so this only carries what's known pre-call, unlike
 *  [[DynamoDBResponseMetadata]].
 *
 *  `PutItem` carries the full `item: Item` rather than a derived key: unlike `GetItem`/
 *  `UpdateItem`/`DeleteItem`, a put has no separate key field to extract without table schema
 *  knowledge the library doesn't have — the caller who built `item` already knows their own
 *  schema and can pull out whatever fields they consider identity themselves.
 *
 *  No `TransactGetItems`/`TransactWriteItems` cases: neither operation retries at all today (no
 *  `retryPolicy` field, no fallback to `defaultRetryPolicy`) — a failed transaction attempt has
 *  no library-safe default response, so retrying (and observing a retry) is out of scope until
 *  transactions grow their own retry story.
 */
sealed trait DynamoDBRetryMetadata

object DynamoDBRetryMetadata {
  final case class GetItem(tableName: String, correlation: CorrelationContext)    extends DynamoDBRetryMetadata
  final case class PutItem(tableName: String, item: Item)                         extends DynamoDBRetryMetadata
  final case class UpdateItem(tableName: String, correlation: CorrelationContext) extends DynamoDBRetryMetadata
  final case class DeleteItem(tableName: String, correlation: CorrelationContext) extends DynamoDBRetryMetadata
  final case class Query(tableName: String)                                       extends DynamoDBRetryMetadata
  final case class Scan(tableName: String)                                        extends DynamoDBRetryMetadata
  final case class BatchGetItem(tableNames: Set[String])                          extends DynamoDBRetryMetadata
  final case class BatchWriteItem(tableNames: Set[String])                        extends DynamoDBRetryMetadata
}
